//! A paused torrent keeps none of its files open, a resumed one opens them again where they
//! are, a file moved away while it was paused is an error rather than a new empty file, and
//! on Windows a finished file is held read-only, so other programs can read, rename and move it
//! while it is seeded.

use std::{
    collections::HashSet,
    net::Ipv4Addr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use tempfile::TempDir;

use super::{
    fake_peer::{
        BLOCK, FakePeer, Fixture, PIECES, Recv, SeederOpts, Wire, peer_id, run, start_seeder,
    },
    test_util::held,
};
use crate::{
    AddTorrent, ConnectionOptions, ListenerOptions, Session, SessionOptions, TorrentStatsState,
    api::TorrentIdOrHash, torrent_state::ManagedTorrentHandle,
};

struct Client {
    session: Arc<Session>,
    handle: ManagedTorrentHandle,
    file: PathBuf,
    _dir: TempDir,
}

/// A session on loopback with the torrent of `seed` added, nothing downloaded yet.
async fn client(seed: &Fixture) -> anyhow::Result<Client> {
    let dir = TempDir::with_prefix("rqbit_release_files")?;
    let session = Session::new_with_opts(
        dir.path().join("download"),
        SessionOptions {
            dht: None,
            persistence: None,
            listen: Some(ListenerOptions {
                listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                ipv4_only: true,
                ..Default::default()
            }),
            connect: Some(ConnectionOptions::default()),
            ipv4_only: true,
            disable_local_service_discovery: true,
            ..Default::default()
        },
    )
    .await?;
    let handle = session
        .add_torrent(AddTorrent::from_bytes(seed.torrent_bytes.clone()), None)
        .await?
        .into_handle()
        .context("into_handle")?;
    handle.wait_until_initialized().await?;
    let file = handle.with_metadata(|m| {
        handle
            .output_folder()
            .join(&m.file_infos[0].relative_filename)
    })?;
    Ok(Client {
        session,
        handle,
        file,
        _dir: dir,
    })
}

/// Serves the client the pieces `bits` says this peer has, until it announced it has `want`.
async fn serve(client: &Client, seed: &Fixture, bits: u8, want: &[u32]) -> anyhow::Result<()> {
    let addr = client.session.listen_addr().context("not listening")?;
    let mut peer = FakePeer::connect(addr, seed.info_hash, &peer_id(b"-FK0001-")).await?;
    peer.bitfield(&[bits]).await?;
    peer.unchoke().await?;
    let mut haves = HashSet::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    while !want.iter().all(|p| haves.contains(p)) {
        match peer
            .recv(deadline.saturating_duration_since(Instant::now()))
            .await
        {
            Recv::Msg(Wire::Request(piece, begin, len)) => {
                peer.piece(piece, begin, seed.block(piece, begin, len))
                    .await?;
            }
            Recv::Msg(Wire::Have(piece)) => {
                haves.insert(piece);
            }
            Recv::Msg(_) => {}
            Recv::Timeout | Recv::Closed => anyhow::bail!("got HAVE only for {haves:?}"),
        }
    }
    Ok(())
}

fn id(client: &Client) -> TorrentIdOrHash {
    client.handle.info_hash().into()
}

// Paused mid-download: no handle on its file, which can be renamed (also on Windows) and put
// back; resumed, it finishes there with the right bytes.
#[tokio::test(flavor = "multi_thread")]
async fn test_paused_torrent_holds_no_file_and_resumes() -> anyhow::Result<()> {
    run(async {
        let seed = start_seeder(SeederOpts::default()).await?;
        let client = client(&seed).await?;
        assert_ne!(held(&client.file), Some(false), "open while it downloads");

        serve(&client, &seed, 0b1110_0000, &[0, 1, 2]).await?;
        client.session.pause(&client.handle).await?;
        assert_ne!(
            held(&client.file),
            Some(true),
            "a paused torrent holds no handle on its file"
        );
        let moved = client.file.with_extension("moved");
        std::fs::rename(&client.file, &moved).context("renaming the paused file")?;
        std::fs::rename(&moved, &client.file)?;

        client.session.unpause(&client.handle).await?;
        // Finished, it drops the peer, maybe before the last HAVE: the bytes tell.
        let _ = serve(&client, &seed, 0b1111_0000, &[3]).await;
        client.handle.wait_until_completed().await?;
        assert_eq!(std::fs::read(&client.file)?, seed.content);
        client.session.delete(id(&client), false).await?;
        Ok(())
    })
    .await
}

// A file moved away while the torrent was paused: resuming stops it with an error that says
// so, and nothing is made at its path.
#[tokio::test(flavor = "multi_thread")]
async fn test_file_moved_while_paused_is_an_error() -> anyhow::Result<()> {
    run(async {
        let seed = start_seeder(SeederOpts::default()).await?;
        let client = client(&seed).await?;
        serve(&client, &seed, 0b1110_0000, &[0, 1, 2]).await?;
        client.session.pause(&client.handle).await?;
        let moved = client.file.with_extension("moved");
        std::fs::rename(&client.file, &moved)?;

        client.session.unpause(&client.handle).await?;
        // The peer is served until the torrent stops: its write is what finds the file gone.
        let _ = serve(&client, &seed, 0b1111_0000, &[3]).await;
        let deadline = Instant::now() + Duration::from_secs(20);
        let error = loop {
            let stats = client.handle.stats();
            if matches!(stats.state, TorrentStatsState::Error) {
                break stats.error.unwrap_or_default();
            }
            anyhow::ensure!(Instant::now() < deadline, "never stopped: {stats}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert!(error.contains("moved or deleted"), "{error}");
        assert!(!client.file.exists(), "nothing made at the file's path");
        assert_eq!(
            std::fs::read(&moved)?[..(3 * super::fake_peer::PIECE_LEN) as usize],
            seed.content[..(3 * super::fake_peer::PIECE_LEN) as usize],
            "the moved file is left as it was"
        );
        Ok(())
    })
    .await
}

// Finished and seeding: every other program can read the file (Windows: even one that keeps
// writers out), rename it while it is seeded, and peers still get its blocks.
#[tokio::test(flavor = "multi_thread")]
async fn test_finished_file_is_shared_while_seeding() -> anyhow::Result<()> {
    run(async {
        let seed = start_seeder(SeederOpts::default()).await?;
        let client = client(&seed).await?;
        let _ = serve(
            &client,
            &seed,
            0b1111_0000,
            &(0..PIECES).collect::<Vec<_>>(),
        )
        .await;
        client.handle.wait_until_completed().await?;

        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            let mut other = Vec::new();
            std::io::Read::read_to_end(
                &mut std::fs::OpenOptions::new()
                    .read(true)
                    .share_mode(0x1) // FILE_SHARE_READ: no writer may hold it
                    .open(&client.file)
                    .context("a reader that keeps writers out")?,
                &mut other,
            )?;
            assert_eq!(other, seed.content);
        }
        let moved = client.file.with_extension("moved");
        std::fs::rename(&client.file, &moved).context("renaming the seeded file")?;

        // Still seeded, from where it is now.
        let addr = client.session.listen_addr().context("not listening")?;
        let mut peer = FakePeer::connect(addr, seed.info_hash, &peer_id(b"-FK0002-")).await?;
        peer.interested().await?;
        peer.request(2, BLOCK, BLOCK).await?;
        let pieces = peer.expect_pieces(1, Duration::from_secs(10)).await?;
        assert_eq!(pieces[0].2, seed.block(2, BLOCK, BLOCK));

        // Paused: nothing held. Started again: opened (read-only on Windows) where it was
        // put back.
        client.session.pause(&client.handle).await?;
        assert_ne!(held(&moved), Some(true));
        std::fs::rename(&moved, &client.file)?;
        client.session.unpause(&client.handle).await?;
        let mut peer = FakePeer::connect(addr, seed.info_hash, &peer_id(b"-FK0003-")).await?;
        peer.interested().await?;
        peer.request(3, 0, BLOCK).await?;
        let pieces = peer.expect_pieces(1, Duration::from_secs(10)).await?;
        assert_eq!(pieces[0].2, seed.block(3, 0, BLOCK));
        #[cfg(windows)]
        assert!(
            std::fs::OpenOptions::new()
                .write(true)
                .open(&client.file)
                .is_err(),
            "no writer while it is seeded"
        );
        client.session.delete(id(&client), false).await?;
        Ok(())
    })
    .await
}
