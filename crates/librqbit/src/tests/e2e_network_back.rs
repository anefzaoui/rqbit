//! A network that drops and comes back (Wi-Fi reconnecting, a wake from sleep, a VPN
//! switching), simulated on loopback: each peer sits behind a link that can be cut (its
//! connections closed, new ones closed as soon as they are accepted, as a dead network fails
//! a dial) and restored. Every dial through a link is counted.
//!
//! A torrent whose peers all died with the network waits out their reconnect backoff (10-20 s
//! after the first failure, then x6, up to an hour) even once the network is back: the
//! session is told (`Session::network_changed`) and dials each dead peer once, at once.

use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::Context;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::watch,
    time::{sleep, timeout},
};

use super::{
    fake_peer::{ClientOpts, Fixture, PIECE_LEN, SeederOpts, run, start_client, start_seeder},
    test_util::wait_until,
};

/// The links to the peers, cut and restored together.
struct Network {
    up: watch::Sender<bool>,
}

/// One peer's link: its address (where the client dials) and how many dials came through.
struct Link {
    addr: SocketAddr,
    dials: Arc<AtomicUsize>,
}

impl Link {
    fn dials(&self) -> usize {
        self.dials.load(Ordering::SeqCst)
    }
}

impl Network {
    fn new() -> Self {
        Network {
            up: watch::Sender::new(true),
        }
    }

    fn cut(&self) {
        self.up.send_replace(false);
    }

    fn restore(&self) {
        self.up.send_replace(true);
    }

    async fn link(&self, to: SocketAddr) -> anyhow::Result<Link> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let addr = listener.local_addr()?;
        let dials = Arc::new(AtomicUsize::new(0));
        let up = self.up.subscribe();
        let counter = dials.clone();
        tokio::spawn(async move {
            while let Ok((mut down, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                if !*up.borrow() {
                    continue;
                }
                let mut up = up.clone();
                tokio::spawn(async move {
                    let Ok(mut peer) = TcpStream::connect(to).await else {
                        return;
                    };
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut down, &mut peer) => {}
                        _ = up.wait_for(|up| !*up) => {}
                    }
                });
            }
        });
        Ok(Link { addr, dials })
    }
}

/// A seeder sending 16 KiB a second (the torrent, 128 KiB, takes 8 s), three links to it, and
/// a client that knows the peer only at those three addresses.
async fn swarm() -> anyhow::Result<(Fixture, Network, Vec<Link>, Fixture)> {
    let seed = start_seeder(SeederOpts {
        upload_bps: Some(16 * 1024),
    })
    .await?;
    let net = Network::new();
    let mut links = Vec::new();
    for _ in 0..3 {
        links.push(net.link(seed.addr()).await?);
    }
    let client = start_client(
        &seed,
        ClientOpts {
            listen: false,
            initial_peers: links.iter().map(|l| l.addr).collect(),
        },
    )
    .await?;
    Ok((seed, net, links, client))
}

fn fetched(client: &Fixture) -> u64 {
    client
        .handle
        .live()
        .map_or(0, |l| l.stats_snapshot().fetched_bytes)
}

fn dials(links: &[Link]) -> Vec<usize> {
    links.iter().map(Link::dials).collect()
}

fn added(now: &[usize], before: &[usize]) -> Vec<usize> {
    now.iter().zip(before).map(|(n, b)| n - b).collect()
}

// The network goes for 25 s: every peer dies, fails its first reconnect and waits a minute or
// more for the next. Back, the torrent stays stalled (as it did before this change) until the
// session is told; then it downloads again within seconds, each peer dialed once.
#[tokio::test(flavor = "multi_thread")]
async fn test_torrent_resumes_at_once_when_the_network_comes_back() -> anyhow::Result<()> {
    run(async {
        let (_seed, net, links, client) = swarm().await?;
        wait_until(
            || {
                anyhow::ensure!(fetched(&client) >= PIECE_LEN as u64);
                Ok(())
            },
            Duration::from_secs(20),
        )
        .await
        .context("the download didn't start")?;

        net.cut();
        let at_cut = dials(&links);
        sleep(Duration::from_secs(25)).await;
        let during_cut = added(&dials(&links), &at_cut);
        // One try each 10-20 s after the drop; the next one is 60-120 s after that.
        assert!(
            during_cut.iter().all(|&n| n <= 1),
            "dials while the network was gone: {during_cut:?}"
        );

        net.restore();
        let before = fetched(&client);
        sleep(Duration::from_secs(3)).await;
        assert_eq!(
            fetched(&client),
            before,
            "resumed by itself: the peers' backoff was expected to keep it waiting"
        );

        let at_change = dials(&links);
        let changed = Instant::now();
        assert!(client.session.network_changed());
        wait_until(
            || {
                anyhow::ensure!(fetched(&client) > before);
                Ok(())
            },
            Duration::from_secs(5),
        )
        .await
        .context("not downloading again 5 s after the network came back")?;
        let resumed = changed.elapsed();

        timeout(
            Duration::from_secs(20),
            client.handle.wait_until_completed(),
        )
        .await
        .context("didn't finish")??;
        let after_change = added(&dials(&links), &at_change);
        assert_eq!(
            after_change,
            vec![1, 1, 1],
            "each dead peer dialed once for the change"
        );
        tracing::info!(?resumed, ?during_cut, "resumed");
        Ok(())
    })
    .await
}

// A change while the network is still gone (an address that came back before its route works)
// costs each dead peer one dial. Its fresh backoff then waits 10-20 s, and the wait scheduled
// when it first died dials nothing (it would have been a third dial). A second change this
// soon does nothing.
#[tokio::test(flavor = "multi_thread")]
async fn test_one_dial_per_dead_peer_per_network_change() -> anyhow::Result<()> {
    run(async {
        let (_seed, net, links, client) = swarm().await?;
        wait_until(
            || {
                anyhow::ensure!(fetched(&client) > 0);
                Ok(())
            },
            Duration::from_secs(20),
        )
        .await
        .context("the download didn't start")?;

        net.cut();
        sleep(Duration::from_secs(1)).await;
        let at_change = dials(&links);
        assert!(client.session.network_changed());
        let changed = Instant::now();
        sleep(Duration::from_secs(2)).await;
        assert_eq!(added(&dials(&links), &at_change), vec![1, 1, 1]);
        assert!(
            !client.session.network_changed(),
            "a second change within NETWORK_CHANGE_MIN_GAP"
        );

        // Before the fresh backoff (10 s at the least) ends: nothing more.
        sleep(Duration::from_secs(9).saturating_sub(changed.elapsed())).await;
        assert_eq!(added(&dials(&links), &at_change), vec![1, 1, 1]);

        // The fresh backoff's one dial (10-20 s after the change), and no other.
        sleep(Duration::from_secs(23).saturating_sub(changed.elapsed())).await;
        assert_eq!(added(&dials(&links), &at_change), vec![2, 2, 2]);
        Ok(())
    })
    .await
}
