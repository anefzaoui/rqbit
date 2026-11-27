use std::collections::HashSet;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::bail;
use backon::ExponentialBuilder;
use backon::Retryable;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::Either;
use futures::stream::BoxStream;
use futures::stream::FuturesUnordered;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::trace;
use tracing::trace_span;
use url::Url;

use crate::tracker_comms_http;
use crate::tracker_comms_udp;
use crate::tracker_comms_udp::UdpTrackerClient;
use librqbit_core::hash_id::Id20;

pub struct TrackerComms {
    info_hash: Id20,
    peer_id: Id20,
    stats: Box<dyn TorrentStatsProvider>,
    force_tracker_interval: Option<Duration>,
    tx: Sender,
    // This MUST be set as trackers don't work with 0 port.
    announce_port: u16,
    reqwest_client: reqwest::Client,
    key: u32,
    // Changes when the client saw the network come back (`TrackerComms::start`).
    network: Option<NetworkChanges>,
}

/// A value that changes each time the client saw the network come back (a new local address,
/// a wake from sleep, answers again after an outage): each tracker is then asked again at once
/// rather than at its next turn, as its peers may be gone or moved. Only the change matters.
pub type NetworkChanges = tokio::sync::watch::Receiver<u64>;

/// After the network comes back, a tracker that answered is asked again no sooner than its
/// `min interval` after that answer, or this when it gave none: a client seen announcing more
/// often may be banned. An outage that needs this is longer than it in most cases, so the
/// announce is at once.
const REANNOUNCE_MIN_GAP: Duration = Duration::from_secs(60);

/// Retries after an HTTP tracker failed: 10 s, doubling, up to 10 minutes (with jitter).
fn http_retry_backoff() -> backon::ExponentialBackoff {
    use backon::BackoffBuilder;
    ExponentialBuilder::new()
        .without_max_times()
        .with_jitter()
        .with_factor(2.)
        .with_min_delay(Duration::from_secs(10))
        .with_max_delay(Duration::from_secs(600))
        .build()
}

/// Waits `wait`; a network change ends the wait early, though not before `not_before`.
async fn wait_or_network_change(
    wait: Duration,
    not_before: Option<Duration>,
    network: &mut Option<NetworkChanges>,
) {
    let start = tokio::time::Instant::now();
    let deadline = start + wait;
    let changed = async {
        let changed = match network.as_mut() {
            Some(rx) => rx.changed().await.is_ok(),
            None => false,
        };
        // No watcher, or it is gone: only the wait ends it.
        if !changed {
            std::future::pending::<()>().await
        }
    };
    tokio::select! {
        _ = tokio::time::sleep_until(deadline) => {}
        _ = changed => {
            let at = not_before.map_or(start, |gap| start + gap).min(deadline);
            let left = at.saturating_duration_since(tokio::time::Instant::now());
            debug!(?left, "network changed, announcing again");
            tokio::time::sleep_until(at).await;
        }
    }
}

#[derive(Default)]
pub enum TrackerCommsStatsState {
    #[default]
    None,
    Initializing,
    Paused,
    Live,
}

#[derive(Default)]
pub struct TrackerCommsStats {
    pub uploaded_bytes: u64,
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
    pub torrent_state: TrackerCommsStatsState,
}

impl TrackerCommsStats {
    pub fn get_left_to_download_bytes(&self) -> u64 {
        let total = self.total_bytes;
        let down = self.downloaded_bytes;
        if total >= down {
            return total - down;
        }
        0
    }

    pub fn is_completed(&self) -> bool {
        self.downloaded_bytes >= self.total_bytes
    }
}

pub trait TorrentStatsProvider: Send + Sync {
    fn get(&self) -> TrackerCommsStats;
}

impl TorrentStatsProvider for () {
    fn get(&self) -> TrackerCommsStats {
        Default::default()
    }
}

type Sender = tokio::sync::mpsc::Sender<SocketAddr>;

enum SupportedTracker {
    Udp(Url),
    Http(Url),
}

impl SupportedTracker {
    fn url(&self) -> &Url {
        match self {
            SupportedTracker::Udp(url) | SupportedTracker::Http(url) => url,
        }
    }
}

impl std::fmt::Debug for SupportedTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SupportedTracker::Udp(u) => std::fmt::Display::fmt(u, f),
            SupportedTracker::Http(u) => std::fmt::Display::fmt(u, f),
        }
    }
}

/// How long to wait before the next announce to an HTTP tracker: its `interval`, never
/// under its `min interval`, and never under 5 seconds (as for UDP trackers), so a tracker
/// that answers 0 isn't asked again in a tight loop.
fn http_announce_interval(interval: u64, min_interval: Option<u64>) -> Duration {
    Duration::from_secs(interval.max(min_interval.unwrap_or(0)).max(5))
}

/// How soon after an answer an HTTP tracker may be asked again when the network comes back:
/// its `min interval`, or `REANNOUNCE_MIN_GAP` when it gave none; never under 5 seconds.
fn http_reannounce_gap(min_interval: Option<u64>) -> Duration {
    min_interval
        .map_or(REANNOUNCE_MIN_GAP, Duration::from_secs)
        .max(Duration::from_secs(5))
}

/// Drives one tracker monitor to its end and logs how it ended. A panic in the monitor ends only
/// that tracker.
async fn run_isolated(url: Url, monitor: impl Future<Output = anyhow::Result<()>>) {
    match std::panic::AssertUnwindSafe(monitor).catch_unwind().await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => debug!(tracker = %url, "error: {e:#}"),
        Err(panic) => {
            let msg = panic
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("<non-string panic payload>");
            tracing::error!(tracker = %url, "tracker monitor panicked, disabling this tracker: {msg}");
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum UdpTrackerResolveResult {
    One(SocketAddr),
    Two(SocketAddrV4, SocketAddrV6),
}

/// A UDP tracker's addresses, of those `allowed` lets through (the client's filter).
async fn udp_tracker_to_socket_addrs(
    host: url::Host<&str>,
    port: u16,
    allowed: impl Fn(SocketAddr) -> bool,
) -> anyhow::Result<UdpTrackerResolveResult> {
    let res = match host {
        url::Host::Domain(name) => {
            // Use the first IPv4 and the first IPv6 addresses only, of the allowed ones.

            let mut v4: Option<SocketAddrV4> = None;
            let mut v6: Option<SocketAddrV6> = None;
            for addr in tokio::net::lookup_host((name, port))
                .await
                .with_context(|| format!("error looking up hostname {name}"))?
                .filter(|addr| allowed(*addr))
            {
                match (v4, v6, addr) {
                    (None, _, SocketAddr::V4(addr)) => v4 = Some(addr),
                    (_, None, SocketAddr::V6(addr)) => v6 = Some(addr),
                    _ => continue,
                }
            }
            let res = match (v4, v6) {
                (Some(v4), Some(v6)) => UdpTrackerResolveResult::Two(v4, v6),
                (Some(v4), None) => UdpTrackerResolveResult::One(v4.into()),
                (None, Some(v6)) => UdpTrackerResolveResult::One(v6.into()),
                _ => anyhow::bail!("no usable addresses returned looking up {name}"),
            };
            trace!(?res, "resolved");
            res
        }
        url::Host::Ipv4(addr) => UdpTrackerResolveResult::One((addr, port).into()),
        url::Host::Ipv6(addr) => UdpTrackerResolveResult::One((addr, port).into()),
    };
    if let UdpTrackerResolveResult::One(addr) = res
        && !allowed(addr)
    {
        anyhow::bail!("tracker address {addr} refused by the filter");
    }
    Ok(res)
}

/// The most of an HTTP tracker's response that is read. A response naming a few hundred peers
/// is a few KB.
const MAX_TRACKER_RESPONSE_BYTES: usize = 2 << 20;

/// A response's body, at most `max` bytes of it (more is an error, read no further).
async fn read_body_capped(mut response: reqwest::Response, max: usize) -> anyhow::Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|len| len > max as u64)
    {
        bail!("tracker response larger than {max} bytes");
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if body.len() + chunk.len() > max {
            bail!("tracker response larger than {max} bytes");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

impl TrackerComms {
    // TODO: fix too many args
    /// `network`: see [`NetworkChanges`]; None keeps each tracker to its own schedule.
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        info_hash: Id20,
        peer_id: Id20,
        trackers: HashSet<Url>,
        stats: Box<dyn TorrentStatsProvider>,
        force_interval: Option<Duration>,
        announce_port: u16,
        reqwest_client: reqwest::Client,
        udp_client: UdpTrackerClient,
        network: Option<NetworkChanges>,
    ) -> Option<BoxStream<'static, SocketAddr>> {
        let trackers = trackers
            .into_iter()
            .filter_map(|t| match t.scheme() {
                "http" | "https" => Some(SupportedTracker::Http(t)),
                "udp" => Some(SupportedTracker::Udp(t)),
                _ => {
                    debug!("unsupported tracker URL: {}", t);
                    None
                }
            })
            .collect::<Vec<_>>();
        if trackers.is_empty() {
            debug!(?info_hash, "trackers list is empty");
            return None;
        }

        tracing::trace!(?trackers);

        let (tx, mut rx) = tokio::sync::mpsc::channel::<SocketAddr>(16);

        let s = async_stream::stream! {
            use futures::StreamExt;
            let comms = Arc::new(Self {
                info_hash,
                peer_id,
                stats,
                force_tracker_interval: force_interval,
                tx,
                announce_port,
                reqwest_client,
                key: rand::random(),
                network,
            });
            // Each tracker is isolated behind `catch_unwind`: all trackers and
            // (upstream of this stream) DHT/LSD peers are polled from the same
            // task, so a panic inside one tracker's I/O stack (e.g. a TLS
            // verifier that aborts instead of returning an error) would
            // otherwise tear down peer discovery for the whole torrent.
            let mut futures = FuturesUnordered::new();
            for tracker in trackers {
                let url = tracker.url().clone();
                futures.push(run_isolated(url, comms.add_tracker(tracker, &udp_client)));
            }
            while !(futures.is_empty()) {
                tokio::select! {
                    addr = rx.recv() => {
                        if let Some(addr) = addr {
                            yield addr;
                        }
                    }
                    _ = futures.next(), if !futures.is_empty() => {}
                }
            }
        };

        Some(s.boxed())
    }

    fn add_tracker(
        &self,
        url: SupportedTracker,
        client: &UdpTrackerClient,
    ) -> Either<
        impl std::future::Future<Output = anyhow::Result<()>> + '_ + Send,
        impl std::future::Future<Output = anyhow::Result<()>> + '_ + Send,
    > {
        let info_hash = self.info_hash;
        match url {
            SupportedTracker::Udp(url) => {
                let span = debug_span!(parent: None, "udp_tracker", tracker = %url, info_hash = ?info_hash);
                self.task_single_tracker_monitor_udp(url, client.clone())
                    .instrument(span)
                    .right_future()
            }
            SupportedTracker::Http(url) => {
                let span = debug_span!(
                    parent: None,
                    "http_tracker",
                    tracker = %url,
                    info_hash = ?info_hash
                );
                self.task_single_tracker_monitor_http(url)
                    .instrument(span)
                    .left_future()
            }
        }
    }

    async fn task_single_tracker_monitor_http(&self, tracker_url: Url) -> anyhow::Result<()> {
        trace!(url=%tracker_url, "starting monitor");
        let mut event = Some(tracker_comms_http::TrackerRequestEvent::Started);
        let mut network = self.network.clone();
        let mut retry = http_retry_backoff();

        loop {
            match self.tracker_one_request_http(&tracker_url, event).await {
                Ok((interval, reannounce_gap)) => {
                    event = None;
                    retry = http_retry_backoff();
                    let interval = self.force_tracker_interval.unwrap_or(interval);
                    debug!("sleeping for {:?} after calling tracker", interval);
                    wait_or_network_change(interval, Some(reannounce_gap), &mut network).await;
                }
                Err(err) => {
                    // The backoff has no end (without_max_times); its longest wait otherwise.
                    let retry_in = retry.next().unwrap_or(Duration::from_secs(600));
                    debug!(?retry_in, "error calling tracker: {err:#}");
                    // It didn't answer: asked again at once when the network comes back.
                    wait_or_network_change(retry_in, None, &mut network).await;
                }
            }
        }
    }

    /// The wait until the next announce, and how soon after this one it may come when the
    /// network comes back.
    async fn tracker_one_request_http(
        &self,
        tracker_url: &Url,
        event: Option<tracker_comms_http::TrackerRequestEvent>,
    ) -> anyhow::Result<(Duration, Duration)> {
        let stats = self.stats.get();
        let request = tracker_comms_http::TrackerRequest {
            info_hash: &self.info_hash,
            peer_id: &self.peer_id,
            port: self.announce_port,
            uploaded: stats.uploaded_bytes,
            downloaded: stats.downloaded_bytes,
            left: stats.get_left_to_download_bytes(),
            compact: true,
            no_peer_id: false,
            event,
            ip: None,
            numwant: None,
            key: Some(self.key),
            trackerid: None,
        };

        let mut url = tracker_url.clone();

        let mut queries = request.as_querystring();
        if let Some(url_query) = url.query() {
            queries.push_str(&format!("&{}", url_query));
        }
        url.set_query(Some(&queries));

        let response: reqwest::Response = self.reqwest_client.get(url).send().await?;
        if !response.status().is_success() {
            anyhow::bail!("tracker responded with {:?}", response.status());
        }
        let bytes = read_body_capped(response, MAX_TRACKER_RESPONSE_BYTES).await?;
        if let Ok((error, _)) =
            bencode::from_bytes_with_rest::<tracker_comms_http::TrackerError>(&bytes)
        {
            anyhow::bail!(
                "tracker returned failure. Failure reason: {}",
                error.failure_reason
            )
        };
        let response = bencode::from_bytes_with_rest::<tracker_comms_http::TrackerResponse>(&bytes)
            .map_err(|e| {
                tracing::trace!("error deserializing TrackerResponse: {e:#}");
                e.into_kind()
            })?
            .0;

        for peer in response.iter_peers() {
            self.tx.send(peer).await?;
        }
        Ok((
            http_announce_interval(response.interval, response.min_interval),
            http_reannounce_gap(response.min_interval),
        ))
    }

    async fn task_single_tracker_monitor_udp(
        &self,
        url: Url,
        client: UdpTrackerClient,
    ) -> anyhow::Result<()> {
        if url.scheme() != "udp" {
            bail!("expected UDP scheme in {}", url);
        }
        let (host, port) = (
            url.host().context("missing host")?,
            url.port().context("missing port")?,
        );

        let mut sleep_interval: Option<Duration> = None;
        // Whether the last announce was answered: then a network change waits
        // REANNOUNCE_MIN_GAP after it (UDP trackers give no `min interval`).
        let mut answered = false;
        let mut network = self.network.clone();
        let mut prev_addrs: Option<UdpTrackerResolveResult> = None;
        loop {
            if let Some(i) = sleep_interval {
                trace!(interval=?sleep_interval, "sleeping");
                let gap = answered.then_some(REANNOUNCE_MIN_GAP);
                wait_or_network_change(i, gap, &mut network).await;
            }

            // This should retry forever until the addrs are resolved.
            let addrs = (async || {
                udp_tracker_to_socket_addrs(host.clone(), port, |a| client.allows(a))
                    .instrument(trace_span!("resolve", ?host))
                    .await
                    .or_else(|err| prev_addrs.ok_or(err))
            })
            .retry(
                ExponentialBuilder::new()
                    .without_max_times()
                    .with_max_delay(Duration::from_secs(60))
                    .with_jitter(),
            )
            .notify(|err, retry| debug!(retry_in=?retry, "error resolving tracker: {err:#}"))
            .await
            .context("this shouldn't happen: failed resolving tracker addrs")?;

            prev_addrs = Some(addrs);

            match addrs {
                UdpTrackerResolveResult::One(addr) => {
                    match self
                        .tracker_one_request_udp(addr, &client)
                        .instrument(trace_span!("udp request", ?addr))
                        .await
                    {
                        Ok(sleep) => {
                            answered = true;
                            sleep_interval = Some(sleep)
                        }
                        Err(_) => {
                            answered = false;
                            sleep_interval = Some(sleep_interval.unwrap_or(Duration::from_secs(60)))
                        }
                    }
                }
                UdpTrackerResolveResult::Two(v4, v6) => {
                    let (r4, r6) = tokio::join!(
                        self.tracker_one_request_udp(v4.into(), &client)
                            .instrument(trace_span!("udp request", addr=?v4)),
                        self.tracker_one_request_udp(v6.into(), &client)
                            .instrument(trace_span!("udp request", addr=?v6))
                    );
                    let r = r4.or(r6).ok();
                    answered = r.is_some();
                    sleep_interval = Some(r.or(sleep_interval).unwrap_or(Duration::from_secs(60)))
                }
            }
        }
    }

    async fn tracker_one_request_udp(
        &self,
        addr: SocketAddr,
        client: &UdpTrackerClient,
    ) -> anyhow::Result<Duration> {
        use tracker_comms_udp::*;

        let stats = self.stats.get();
        let request = AnnounceFields {
            info_hash: self.info_hash,
            peer_id: self.peer_id,
            downloaded: stats.downloaded_bytes,
            left: stats.get_left_to_download_bytes(),
            uploaded: stats.uploaded_bytes,
            event: match stats.torrent_state {
                TrackerCommsStatsState::None => EVENT_NONE,
                TrackerCommsStatsState::Initializing => EVENT_STARTED,
                TrackerCommsStatsState::Paused => EVENT_STOPPED,
                TrackerCommsStatsState::Live => {
                    if stats.is_completed() {
                        EVENT_COMPLETED
                    } else {
                        EVENT_STARTED
                    }
                }
            },
            key: self.key,
            port: self.announce_port,
        };

        match client.announce(addr, request).await {
            Ok(response) => {
                trace!(len = response.addrs.len(), "received announce response");
                for addr in response.addrs {
                    self.tx.send(addr).await.context("rx closed")?;
                }
                let sleep = response.interval.max(5);
                let sleep = Duration::from_secs(sleep as u64);
                Ok(sleep)
            }
            Err(e) => {
                debug!(?addr, "error reading announce response: {e:#}");
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
    use url::Url;

    use super::{
        UdpTrackerResolveResult, http_announce_interval, read_body_capped, run_isolated,
        udp_tracker_to_socket_addrs,
    };

    /// A server on loopback that answers one request with `body`, with or without its length.
    async fn answer_once(body: Vec<u8>, with_length: bool) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut req = [0u8; 1024];
            let _ = s.read(&mut req).await;
            let head = if with_length {
                format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len())
            } else {
                "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n".to_owned()
            };
            let _ = s.write_all(head.as_bytes()).await;
            let _ = s.write_all(&body).await;
            let _ = s.shutdown().await;
        });
        format!("http://{addr}/announce")
    }

    #[tokio::test]
    async fn a_tracker_response_is_read_up_to_the_cap() {
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        for with_length in [true, false] {
            let url = answer_once(b"d8:intervali60ee".to_vec(), with_length).await;
            let r = client.get(url).send().await.unwrap();
            assert_eq!(read_body_capped(r, 16).await.unwrap(), b"d8:intervali60ee");

            let url = answer_once(vec![b'x'; 100_000], with_length).await;
            let r = client.get(url).send().await.unwrap();
            let err = read_body_capped(r, 1000).await.unwrap_err();
            assert!(format!("{err:#}").contains("larger than"), "{err:#}");
        }
    }

    #[tokio::test]
    async fn a_udp_trackers_addresses_are_only_the_allowed_ones() {
        let not_loopback = |a: SocketAddr| !a.ip().is_loopback();
        let ip4 = |s: &str| url::Host::Ipv4(s.parse().unwrap());
        for refused in [
            url::Host::Domain("localhost"),
            ip4("127.0.0.1"),
            url::Host::Ipv6("::1".parse().unwrap()),
        ] {
            let r = udp_tracker_to_socket_addrs(refused.clone(), 6969, not_loopback).await;
            assert!(r.is_err(), "{refused:?}: {r:?}");
        }
        let all = |_: SocketAddr| true;
        assert!(matches!(
            udp_tracker_to_socket_addrs(ip4("127.0.0.1"), 6969, all).await,
            Ok(UdpTrackerResolveResult::One(a)) if a == "127.0.0.1:6969".parse().unwrap()
        ));
        assert!(
            udp_tracker_to_socket_addrs(url::Host::Domain("localhost"), 6969, all)
                .await
                .is_ok()
        );
        assert!(matches!(
            udp_tracker_to_socket_addrs(ip4("203.0.113.7"), 6969, not_loopback).await,
            Ok(UdpTrackerResolveResult::One(_))
        ));
    }

    fn isolated(
        url: &str,
        monitor: impl Future<Output = anyhow::Result<()>> + Send + 'static,
    ) -> BoxFuture<'static, ()> {
        run_isolated(Url::parse(url).unwrap(), monitor).boxed()
    }

    // A tracker monitor that panics (e.g. a TLS verifier that panics instead of returning an
    // error) ends only that tracker: the other monitors, polled by the same task, keep running.
    #[tokio::test]
    async fn test_panicking_tracker_does_not_affect_other_trackers() {
        let finished = Arc::new(AtomicUsize::new(0));
        let mut monitors = FuturesUnordered::new();
        monitors.push(isolated("https://panics-str.example/announce", async {
            if true {
                panic!("verifier is not initialized");
            }
            Ok(())
        }));
        monitors.push(isolated("https://panics-string.example/announce", async {
            if true {
                panic!("{}", String::from("formatted panic"));
            }
            Ok(())
        }));
        monitors.push(isolated("https://fails.example/announce", async {
            anyhow::bail!("connection refused")
        }));
        for name in ["a", "b"] {
            let finished = finished.clone();
            monitors.push(isolated(
                &format!("https://{name}.example/announce"),
                async move {
                    tokio::task::yield_now().await;
                    finished.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
            ));
        }
        let mut done = 0;
        while monitors.next().await.is_some() {
            done += 1;
        }
        assert_eq!(
            done, 5,
            "every monitor ends, none of them unwinds into the poller"
        );
        assert_eq!(finished.load(Ordering::SeqCst), 2);
    }

    /// An HTTP tracker on loopback: answers request n (from 0) with `answer(n)` (a status and a
    /// body) and sends each request's time on the channel.
    async fn counting_tracker(
        answer: impl Fn(usize) -> (u16, Vec<u8>) + Send + Sync + 'static,
    ) -> (
        String,
        tokio::sync::mpsc::UnboundedReceiver<tokio::time::Instant>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut n = 0;
            loop {
                let (mut s, _) = listener.accept().await.unwrap();
                let mut req = [0u8; 2048];
                let _ = s.read(&mut req).await;
                let _ = tx.send(tokio::time::Instant::now());
                let (status, body) = answer(n);
                n += 1;
                let head = format!(
                    "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = s.write_all(head.as_bytes()).await;
                let _ = s.write_all(&body).await;
                let _ = s.shutdown().await;
            }
        });
        (format!("http://{addr}/announce"), rx)
    }

    /// One HTTP tracker's monitor, with `network` for its network changes. The peers it
    /// receives are dropped.
    fn monitor_http(url: &str, network: super::NetworkChanges) {
        let (tx, mut peers) = tokio::sync::mpsc::channel(16);
        tokio::spawn(async move { while peers.recv().await.is_some() {} });
        let comms = Arc::new(super::TrackerComms {
            info_hash: Default::default(),
            peer_id: Default::default(),
            stats: Box::new(()),
            force_tracker_interval: None,
            tx,
            announce_port: 6881,
            reqwest_client: reqwest::Client::builder().no_proxy().build().unwrap(),
            key: 1,
            network: Some(network),
        });
        let url = Url::parse(url).unwrap();
        tokio::spawn(async move { comms.task_single_tracker_monitor_http(url).await });
    }

    /// Whether a request comes within `secs` seconds.
    async fn asked_within(
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<tokio::time::Instant>,
        secs: f64,
    ) -> bool {
        tokio::time::timeout(std::time::Duration::from_secs_f64(secs), rx.recv())
            .await
            .is_ok()
    }

    // When the network comes back, a tracker that answered (here 30 minutes to the next
    // announce) is asked again at once, once its `min interval` has passed; and only once per
    // change. With a `min interval` longer than the outage, not before it.
    #[tokio::test]
    async fn test_http_tracker_asked_again_when_the_network_comes_back() {
        for (min_interval, asked_again) in [(1, true), (3600, false)] {
            let body = format!("d8:intervali1800e12:min intervali{min_interval}e5:peers0:e");
            let (url, mut asked) =
                counting_tracker(move |_| (200, body.clone().into_bytes())).await;
            let (network, rx) = tokio::sync::watch::channel(0u64);
            monitor_http(&url, rx);
            assert!(asked_within(&mut asked, 5.).await, "the first announce");
            // Its own schedule: nothing for 30 minutes. (Past the 5 s floor of a 1 s min
            // interval, so the change below is answered at once.)
            assert!(
                !asked_within(&mut asked, 5.).await,
                "asked again unprompted"
            );

            network.send_modify(|n| *n += 1);
            assert_eq!(
                asked_within(&mut asked, 2.).await,
                asked_again,
                "min interval {min_interval}"
            );
            assert!(
                !asked_within(&mut asked, 2.).await,
                "asked twice for one change"
            );
        }
    }

    // A tracker that failed waits 10-20 s for its next try; when the network comes back it is
    // asked at once.
    #[tokio::test]
    async fn test_failed_http_tracker_retried_when_the_network_comes_back() {
        let (url, mut asked) = counting_tracker(|n| match n {
            0 => (503, b"down".to_vec()),
            _ => (200, b"d8:intervali1800e5:peers0:e".to_vec()),
        })
        .await;
        let (network, rx) = tokio::sync::watch::channel(0u64);
        monitor_http(&url, rx);
        assert!(asked_within(&mut asked, 5.).await, "the first announce");
        assert!(
            !asked_within(&mut asked, 2.).await,
            "retried before its backoff"
        );
        network.send_modify(|n| *n += 1);
        assert!(asked_within(&mut asked, 2.).await, "not retried at once");
        assert!(
            !asked_within(&mut asked, 3.).await,
            "asked twice for one change"
        );
    }

    #[test]
    fn test_http_reannounce_gap() {
        use std::time::Duration;
        assert_eq!(
            super::http_reannounce_gap(Some(900)),
            Duration::from_secs(900)
        );
        assert_eq!(super::http_reannounce_gap(None), super::REANNOUNCE_MIN_GAP);
        assert_eq!(super::http_reannounce_gap(Some(0)), Duration::from_secs(5));
    }

    #[test]
    fn test_http_announce_interval() {
        use std::time::Duration;
        // `interval` is the normal wait; `min interval` is only a floor (BEP 3 and the
        // common extension), it used to be taken in its place.
        assert_eq!(
            http_announce_interval(1800, Some(900)),
            Duration::from_secs(1800)
        );
        assert_eq!(
            http_announce_interval(1800, None),
            Duration::from_secs(1800)
        );
        assert_eq!(
            http_announce_interval(600, Some(900)),
            Duration::from_secs(900)
        );
        // A tracker that answers 0 isn't asked again at once.
        assert_eq!(http_announce_interval(0, None), Duration::from_secs(5));
        assert_eq!(http_announce_interval(0, Some(0)), Duration::from_secs(5));
    }
}
