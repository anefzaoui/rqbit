// A pause that lands while a piece's hash is being checked.
//
// When a piece's last chunk arrives, the piece leaves the in-flight list, its hash is checked
// with no lock held, and only then is it marked as had. A pause in between used to hand back
// the in-flight pieces only, so that piece came back neither had, queued nor in flight, with
// all its chunks marked as received: it was never asked for again, and the torrent stayed one
// piece short however many times it was resumed.

use std::{
    net::Ipv4Addr,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::Duration,
};

use anyhow::Context;
use parking_lot::Mutex;
use tempfile::TempDir;
use tokio::{sync::oneshot, time::timeout};
use tracing::info;

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, ManagedTorrentShared, Session,
    SessionOptions, TorrentMetadata, create_torrent,
    spawn_utils::BlockingSpawner,
    storage::{
        BoxStorageFactory, StorageFactory, StorageFactoryExt, TorrentStorage,
        filesystem::FilesystemStorageFactory,
    },
    tests::test_util::{TestPeerMetadata, create_default_random_dir_with_torrents},
};

/// Holds the first read made once armed - on a downloading torrent, the one of a piece's hash
/// check - until the test lets it go.
#[derive(Default)]
struct Gate {
    armed: AtomicBool,
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<Option<mpsc::Receiver<()>>>,
    left: Mutex<Option<oneshot::Sender<()>>>,
}

impl Gate {
    fn hold_if_armed(&self) {
        if !self.armed.swap(false, Ordering::SeqCst) {
            return;
        }
        if let Some(tx) = self.entered.lock().take() {
            let _ = tx.send(());
        }
        if let Some(rx) = self.release.lock().take() {
            let _ = rx.recv_timeout(Duration::from_secs(30));
        }
        if let Some(tx) = self.left.lock().take() {
            let _ = tx.send(());
        }
    }
}

#[derive(Clone)]
struct GatedStorageFactory {
    gate: Arc<Gate>,
}

impl StorageFactory for GatedStorageFactory {
    type Storage = GatedStorage;

    fn create(
        &self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<GatedStorage> {
        Ok(GatedStorage {
            inner: Box::new(FilesystemStorageFactory::default().create(shared, metadata)?),
            gate: self.gate.clone(),
        })
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}

struct GatedStorage {
    inner: Box<dyn TorrentStorage>,
    gate: Arc<Gate>,
}

impl TorrentStorage for GatedStorage {
    fn init(
        &mut self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<()> {
        self.inner.init(shared, metadata)
    }

    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        // The data is read first, so the check itself succeeds: the piece is good, only the
        // pause comes in between.
        self.inner.pread_exact(file_id, offset, buf)?;
        self.gate.hold_if_armed();
        Ok(())
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        self.inner.pwrite_all(file_id, offset, buf)
    }

    fn remove_file(&self, file_id: usize, filename: &Path) -> anyhow::Result<()> {
        self.inner.remove_file(file_id, filename)
    }

    fn remove_directory_if_empty(&self, path: &Path) -> anyhow::Result<()> {
        self.inner.remove_directory_if_empty(path)
    }

    fn ensure_file_length(&self, file_id: usize, length: u64) -> anyhow::Result<()> {
        self.inner.ensure_file_length(file_id, length)
    }

    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        Ok(Box::new(GatedStorage {
            inner: self.inner.take()?,
            gate: self.gate.clone(),
        }))
    }

    fn on_piece_completed(
        &self,
        piece_index: librqbit_core::lengths::ValidPieceIndex,
    ) -> anyhow::Result<()> {
        self.inner.on_piece_completed(piece_index)
    }
}

async fn pause_during_hash_check() -> anyhow::Result<()> {
    crate::tests::test_util::setup_test_logging();

    let files = create_default_random_dir_with_torrents(1, 64 * 1024, Some("test_pause_check"));
    let torrent = create_torrent(
        files.path(),
        CreateTorrentOptions {
            name: None,
            piece_length: Some(16 * 1024),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await?;
    let orig_content = std::fs::read(files.path().join("0.data"))?;

    let server_session = Session::new_with_opts(
        files.path().into(),
        SessionOptions {
            dht: None,
            peer_id: Some(TestPeerMetadata::good().as_peer_id()),
            persistence: None,
            listen: Some(crate::listen::ListenerOptions {
                listen_addr: (Ipv4Addr::LOCALHOST, 16107).into(),
                ..Default::default()
            }),
            disable_local_service_discovery: true,
            ..Default::default()
        },
    )
    .await
    .context("error creating server session")?;
    server_session
        .add_torrent(
            AddTorrent::from_bytes(torrent.as_bytes()?),
            Some(AddTorrentOptions {
                output_folder: Some(files.path().to_str().unwrap().to_owned()),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("server handle")?
        .wait_until_completed()
        .await?;
    let peer = server_session
        .listen_addr()
        .context("expected listen_addr to be set")?;
    info!("server torrent was completed");

    let client_dir = TempDir::with_prefix("test_pause_check_client")?;
    let client_session = Session::new_with_opts(
        client_dir.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            peer_id: Some(TestPeerMetadata::good().as_peer_id()),
            disable_local_service_discovery: true,
            ..Default::default()
        },
    )
    .await?;

    let gate = Arc::new(Gate::default());
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (left_tx, left_rx) = oneshot::channel();
    *gate.entered.lock() = Some(entered_tx);
    *gate.release.lock() = Some(release_rx);
    *gate.left.lock() = Some(left_tx);

    // Added paused, so the gate is armed only once the initial check is over.
    let client = client_session
        .add_torrent(
            AddTorrent::from_bytes(torrent.as_bytes()?),
            Some(AddTorrentOptions {
                paused: true,
                output_folder: Some(client_dir.path().to_str().unwrap().to_owned()),
                initial_peers: Some(vec![peer]),
                storage_factory: Some(GatedStorageFactory { gate: gate.clone() }.boxed()),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("client handle")?;
    client.wait_until_initialized().await?;
    gate.armed.store(true, Ordering::SeqCst);
    client_session.unpause(&client).await?;

    // A piece's last chunk is in, its hash is being checked: pause now.
    timeout(Duration::from_secs(10), entered_rx)
        .await
        .context("no piece was checked")??;
    client_session.pause(&client).await?;
    info!("paused during a piece's hash check");
    release_tx.send(())?;
    timeout(Duration::from_secs(10), left_rx)
        .await
        .context("the held check never went on")??;
    // The cut-short check finishes against a paused torrent.
    tokio::time::sleep(Duration::from_millis(200)).await;

    client_session.unpause(&client).await?;
    timeout(Duration::from_secs(20), client.wait_until_completed())
        .await
        .context("the torrent never completed after the pause")??;

    let got = std::fs::read(client_dir.path().join("0.data"))?;
    assert!(got == orig_content, "contents differ");

    server_session.stop().await;
    client_session.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pause_during_hash_check_completes_after_resume() -> anyhow::Result<()> {
    timeout(Duration::from_secs(60), pause_during_hash_check()).await?
}
