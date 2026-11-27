//! Files are made only when their first byte is written, and a torrent of thousands of files
//! keeps no more handles open than its budget (ikatson/rqbit#484, #525).
//!
//! Two real sessions on 127.0.0.1: a seeder of a folder, and a client downloading it.

use std::{
    net::{Ipv4Addr, SocketAddr},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::Context;
use bytes::Bytes;
use tempfile::TempDir;
use tokio::time::timeout;

use crate::{
    AddTorrent, AddTorrentOptions, ConnectionOptions, CreateTorrentOptions, ListenerMode,
    ListenerOptions, Session, SessionOptions, SessionPersistenceConfig,
    spawn_utils::BlockingSpawner,
    storage::{
        StorageFactoryExt,
        filesystem::{FilesystemStorageFactory, open_files::OpenFiles},
    },
    torrent_state::ManagedTorrentHandle,
};

const PIECE: u32 = 16384;

fn budget(max: usize) -> &'static OpenFiles {
    Box::leak(Box::new(OpenFiles::new(max)))
}

/// Writes `files` (relative path, bytes) under `dir`.
fn write_files(dir: &Path, files: &[(String, Vec<u8>)]) {
    for (name, bytes) in files {
        let p = dir.join(name);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, bytes).unwrap();
    }
}

fn options(budget: &'static OpenFiles, listen: bool) -> SessionOptions {
    SessionOptions {
        dht: None,
        listen: listen.then(|| ListenerOptions {
            mode: ListenerMode::TcpOnly,
            listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
            ipv4_only: true,
            ..Default::default()
        }),
        connect: Some(ConnectionOptions::default()),
        ipv4_only: true,
        disable_local_service_discovery: true,
        default_storage_factory: Some(FilesystemStorageFactory::with_open_files(budget).boxed()),
        ..Default::default()
    }
}

/// A seeder of `src` (a folder of the torrent's files) on loopback: its torrent and address.
async fn seed(src: &Path, budget: &'static OpenFiles) -> anyhow::Result<(Arc<Session>, Bytes, SocketAddr)> {
    let made = crate::create_torrent(
        src,
        CreateTorrentOptions {
            piece_length: Some(PIECE),
            ..Default::default()
        },
        &BlockingSpawner::new(2),
    )
    .await?;
    let torrent = made.as_bytes()?;
    let seeder = Session::new_with_opts(src.parent().unwrap().to_owned(), options(budget, true)).await?;
    let handle = seeder
        .add_torrent(
            AddTorrent::TorrentFileBytes(torrent.clone()),
            Some(AddTorrentOptions {
                overwrite: true,
                output_folder: Some(src.to_str().unwrap().to_owned()),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("into_handle")?;
    timeout(Duration::from_secs(60), handle.wait_until_completed())
        .await
        .context("the seeder never checked its files")??;
    let addr = seeder.listen_addr().context("seeder not listening")?;
    Ok((seeder, torrent, addr))
}

async fn add(
    client: &Arc<Session>,
    torrent: &Bytes,
    out: &Path,
    peer: Option<SocketAddr>,
    only_files: Option<Vec<usize>>,
    paused: bool,
) -> anyhow::Result<ManagedTorrentHandle> {
    client
        .add_torrent(
            AddTorrent::TorrentFileBytes(torrent.clone()),
            Some(AddTorrentOptions {
                initial_peers: peer.map(|p| vec![p]),
                output_folder: Some(out.to_str().unwrap().to_owned()),
                overwrite: true,
                only_files,
                paused,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("into_handle")
}

/// Every file and folder under `dir`, relative, sorted.
fn listing(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = walkdir::WalkDir::new(dir)
        .min_depth(1)
        .into_iter()
        .filter_map(|e| e.ok())
        .map(|e| {
            e.path()
                .strip_prefix(dir)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/")
        })
        .collect();
    v.sort();
    v
}

fn bytes(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed)).collect()
}

// Only c.bin ticked. Its first piece starts in a.bin and takes all of b.txt, its last piece
// ends in d.bin: those three are made, holding only the bytes of those pieces; sub/e.bin shares
// no piece with c.bin and is never made, nor its folder. Before any byte comes (added paused,
// checked), nothing at all is made. A restart (fastresume) finds it finished and makes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn test_unticked_files_are_made_only_for_shared_pieces() -> anyhow::Result<()> {
    timeout(Duration::from_secs(120), async {
        let root = TempDir::with_prefix("rqbit_lazy_untick")?;
        let src = root.path().join("seed").join("set");
        let files: Vec<(String, Vec<u8>)> = vec![
            ("a.bin".into(), bytes(50_000, 1)),
            ("b.txt".into(), bytes(100, 2)),
            ("c.bin".into(), bytes(50_000, 3)),
            ("d.bin".into(), bytes(50_000, 4)),
            ("sub/e.bin".into(), bytes(30_000, 5)),
        ];
        write_files(&src, &files);
        let (seeder, torrent, addr) = seed(&src, budget(64)).await?;

        let out = root.path().join("got");
        let state = root.path().join("state");
        let mut opts = options(budget(64), false);
        opts.fastresume = true;
        opts.persistence = Some(SessionPersistenceConfig::Json {
            folder: Some(state.clone()),
        });
        let client = Session::new_with_opts(out.clone(), opts).await?;
        let handle = add(&client, &torrent, &out, Some(addr), Some(vec![2]), true).await?;
        handle.wait_until_initialized().await?;
        let names = handle.with_metadata(|m| {
            m.file_infos
                .iter()
                .map(|f| f.relative_filename.to_string_lossy().replace('\\', "/"))
                .collect::<Vec<_>>()
        })?;
        assert_eq!(names, ["a.bin", "b.txt", "c.bin", "d.bin", "sub/e.bin"]);
        assert!(
            !out.exists() || listing(&out).is_empty(),
            "checked, nothing downloaded: nothing made, {:?}",
            listing(&out)
        );

        client.unpause(&handle).await?;
        timeout(Duration::from_secs(60), handle.wait_until_completed())
            .await
            .context("download timed out")??;

        assert_eq!(listing(&out), ["a.bin", "b.txt", "c.bin", "d.bin"]);
        assert_eq!(std::fs::read(out.join("c.bin"))?, files[2].1);
        // a.bin: only from c.bin's first piece on (piece 3 starts at 49152).
        let a = std::fs::read(out.join("a.bin"))?;
        assert_eq!(a.len(), 50_000);
        assert_eq!(a[49_152..], files[0].1[49_152..]);
        assert!(a[..49_152].iter().all(|b| *b == 0), "nothing else of a.bin");
        assert_eq!(std::fs::read(out.join("b.txt"))?, files[1].1);
        // d.bin starts at 100100; c.bin's last piece (6) ends at 114688.
        let d = std::fs::read(out.join("d.bin"))?;
        assert_eq!(d.len(), 114_688 - 100_100);
        assert_eq!(d, files[3].1[..d.len()]);

        // Restarted: resumed from its bitfield, finished without a peer, nothing more made.
        let info_hash = handle.info_hash();
        drop(handle);
        client.stop().await;
        drop(client);
        let mut opts = options(budget(64), false);
        opts.fastresume = true;
        opts.persistence = Some(SessionPersistenceConfig::Json {
            folder: Some(state.clone()),
        });
        let again = Session::new_with_opts(out.clone(), opts).await?;
        let handle = again
            .get(info_hash.into())
            .context("the torrent was not restored")?;
        timeout(Duration::from_secs(30), handle.wait_until_completed())
            .await
            .context("not finished after the restart")??;
        assert_eq!(listing(&out), ["a.bin", "b.txt", "c.bin", "d.bin"]);
        assert_eq!(std::fs::read(out.join("c.bin"))?, files[2].1);
        again.stop().await;
        seeder.stop().await;
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("timed out")?
}

// 5,000 files of 1 to 300 bytes, all ticked, with a budget of 32 handles on each side: the
// client never has more than 32 open (Linux: the process's own count of descriptors into the
// download folder agrees), and every file comes whole.
#[tokio::test(flavor = "multi_thread")]
async fn test_many_tiny_files_stay_within_the_budget() -> anyhow::Result<()> {
    timeout(Duration::from_secs(300), async {
        const FILES: usize = 5_000;
        const MAX: usize = 32;
        let root = TempDir::with_prefix("rqbit_lazy_many")?;
        let src = root.path().join("seed").join("many");
        let files: Vec<(String, Vec<u8>)> = (0..FILES)
            .map(|i| {
                (
                    format!("d{:02}/f{i:05}.bin", i % 50),
                    bytes(1 + (i * 37) % 300, i as u8),
                )
            })
            .collect();
        write_files(&src, &files);
        let seeder_budget = budget(MAX);
        let (seeder, torrent, addr) = seed(&src, seeder_budget).await?;
        assert!(seeder_budget.open_count() <= MAX);

        let out = root.path().join("got");
        let client_budget = budget(MAX);
        let client = Session::new_with_opts(out.clone(), options(client_budget, false)).await?;

        let most = Arc::new(AtomicUsize::new(0));
        let most_fds = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(AtomicBool::new(false));
        let watcher = {
            let (most, most_fds, done, out) =
                (most.clone(), most_fds.clone(), done.clone(), out.clone());
            std::thread::spawn(move || {
                while !done.load(Ordering::Relaxed) {
                    most.fetch_max(client_budget.open_count(), Ordering::Relaxed);
                    if let Some(n) = fds_under(&out) {
                        most_fds.fetch_max(n, Ordering::Relaxed);
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
            })
        };
        let handle = add(&client, &torrent, &out, Some(addr), None, false).await?;
        let r = timeout(Duration::from_secs(240), handle.wait_until_completed()).await;
        done.store(true, Ordering::Relaxed);
        watcher.join().unwrap();
        r.context("download timed out")??;

        let most = most.load(Ordering::Relaxed);
        assert!(most > 0 && most <= MAX, "at most {MAX} open, saw {most}");
        let most_fds = most_fds.load(Ordering::Relaxed);
        assert!(most_fds <= MAX, "descriptors into the folder: {most_fds}");
        for (name, want) in &files {
            assert_eq!(&std::fs::read(out.join(name))?, want, "{name}");
        }
        client.stop().await;
        seeder.stop().await;
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("timed out")?
}

/// Linux: how many of the process's descriptors are files under `dir`.
fn fds_under(dir: &Path) -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        let dir = std::fs::canonicalize(dir).ok()?;
        Some(
            std::fs::read_dir("/proc/self/fd")
                .ok()?
                .filter_map(|e| std::fs::read_link(e.ok()?.path()).ok())
                .filter(|p| p.starts_with(&dir))
                .count(),
        )
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = dir;
        None
    }
}
