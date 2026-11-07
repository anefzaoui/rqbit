//! `SessionOptions::configure_http_client` reaches the trackers' HTTP client.

use std::{sync::Arc, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, Session, SessionOptions, create_torrent,
    spawn_utils::BlockingSpawner,
    tests::test_util::{create_default_random_dir_with_torrents, setup_test_logging},
};

#[tokio::test]
async fn trackers_are_asked_with_the_configured_client() -> anyhow::Result<()> {
    setup_test_logging();
    // A tracker that hands back the first announce it gets (no peers).
    let tracker = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);
    let announce = tokio::spawn(async move {
        let (mut s, _) = tracker.accept().await?;
        let mut req = vec![0u8; 4096];
        let n = s.read(&mut req).await?;
        let body = b"d8:intervali60e5:peers0:e";
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        s.write_all(head.as_bytes()).await?;
        s.write_all(body).await?;
        anyhow::Ok(String::from_utf8_lossy(&req[..n]).to_lowercase())
    });

    let files = create_default_random_dir_with_torrents(1, 8192, Some("test_http_client_hook"));
    let torrent = create_torrent(
        files.path(),
        CreateTorrentOptions {
            trackers: vec![url],
            piece_length: Some(1024),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await?;
    let session = Session::new_with_opts(
        files.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            listen: None,
            disable_local_service_discovery: true,
            configure_http_client: Some(Arc::new(|b: reqwest::ClientBuilder| {
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert("x-configured", "yes".parse().unwrap());
                b.default_headers(headers)
            })),
            ..Default::default()
        },
    )
    .await?;
    session
        .add_torrent(
            AddTorrent::from_bytes(torrent.as_bytes()?),
            Some(AddTorrentOptions {
                output_folder: Some(files.path().to_str().unwrap().to_owned()),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await?;
    let request = timeout(Duration::from_secs(10), announce).await???;
    assert!(request.starts_with("get /announce?"), "{request}");
    assert!(request.contains("x-configured: yes"), "{request}");
    session.stop().await;
    Ok(())
}
