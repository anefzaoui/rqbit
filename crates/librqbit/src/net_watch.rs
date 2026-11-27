//! Notices the network coming back, for [`Session::network_changed`].
//!
//! Polled, so it works the same on Windows, Linux and macOS with no system-specific code (a
//! system's own "network changed" signal could call `Session::network_changed` too; this stays
//! as the fallback). Every [`POLL_INTERVAL`] it reads:
//! - the local addresses the system would send from to the internet: a UDP socket "connected"
//!   to a documentation address (RFC 5737, RFC 3849). Nothing is sent: the system only picks a
//!   route, or fails when it has none. A Wi-Fi that reconnects, another network, a VPN that
//!   comes up or switches: an address appears or changes;
//! - how much wall-clock time went by since the last poll: a wake from sleep shows as a jump;
//! - the DHT's answer and timeout counts: an outage with the link still up (the router lost its
//!   line) shows as requests timing out with no answer, and its first answer after that as the
//!   network back.
//!
//! Not seen: an outage shorter than [`DHT_SILENT_WINDOW`] behind a link that stays up, without
//! a DHT (behind a proxy), or on an adapter the session is bound to by name
//! (`bind_device_name`) while the system's default route stays on another; those peers come
//! back with their own backoff, as before.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Weak,
    time::{Duration, Instant, SystemTime},
};

use tracing::{debug, info};

use crate::Session;

/// How often the watcher looks. Two syscalls and a few loads each time, so cheap; a network
/// that comes back is noticed within this.
pub(crate) const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Wall-clock time between two polls longer than the poll interval by this much: the computer
/// slept (or the process was stopped, or the clock was set forward; each costs one round of
/// reconnects at worst). Above the few seconds a loaded machine may be late by.
pub(crate) const WAKE_GAP: Duration = Duration::from_secs(10);

/// The DHT counts as having been gone when its next answer comes this long after the one
/// before, with requests timing out meanwhile. Above `dht`'s REQUERY_INTERVAL (60 s): every
/// running lookup asks its nodes once a minute, so a healthy DHT with a torrent answers in
/// every such window, even when many of its nodes don't.
pub(crate) const DHT_SILENT_WINDOW: Duration = Duration::from_secs(90);

/// ... and at least this many requests timed out in that stretch: a DHT with nothing to ask
/// (no torrent) is quiet without being gone.
pub(crate) const DHT_SILENT_TIMEOUTS: u64 = 4;

/// The local addresses the system would use to reach the internet, by family.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Routes {
    pub v4: Option<Ipv4Addr>,
    pub v6: Option<Ipv6Addr>,
}

impl Routes {
    /// Read from the system now. `bind_ip`: the session's own address, when it has one: then
    /// only that family, from that address (gone when the address is).
    pub fn read(bind_ip: Option<IpAddr>) -> Self {
        let v4 = route_source((Ipv4Addr::UNSPECIFIED, 0).into(), "192.0.2.1:9", bind_ip);
        let v6 = route_source(
            (Ipv6Addr::UNSPECIFIED, 0).into(),
            "[2001:db8::1]:9",
            bind_ip,
        );
        Routes {
            v4: v4.and_then(|ip| match ip {
                IpAddr::V4(ip) => Some(ip),
                IpAddr::V6(_) => None,
            }),
            v6: v6.and_then(|ip| match ip {
                IpAddr::V6(ip) => Some(ip),
                IpAddr::V4(_) => None,
            }),
        }
    }

    fn any(&self) -> bool {
        self.v4.is_some() || self.v6.is_some()
    }

    /// Whether `self` (now) is a network that came or changed since `before`. Losing one is
    /// not (nothing can be done until it is back). An IPv6 address that changes within the
    /// same /64 is a privacy address rotating (RFC 8981, about once a day), the same network.
    fn came_or_changed_since(&self, before: &Routes) -> bool {
        let v4 = matches!((before.v4, self.v4), (old, Some(new)) if old != Some(new));
        let v6 = match (before.v6, self.v6) {
            (None, Some(_)) => true,
            (Some(old), Some(new)) => old.segments()[..4] != new.segments()[..4],
            _ => false,
        };
        v4 || v6
    }
}

/// The local address a UDP socket bound to `bind` (or to `bind_ip`, when it has `bind`'s
/// family) gets when connected to `target`: the source of the route the system would take.
/// None when there is no route (or the address is gone).
fn route_source(bind: SocketAddr, target: &str, bind_ip: Option<IpAddr>) -> Option<IpAddr> {
    let target: SocketAddr = target.parse().ok()?;
    let bind = match bind_ip {
        Some(ip) if ip.is_ipv4() == bind.is_ipv4() => SocketAddr::new(ip, 0),
        Some(_) => return None,
        None => bind,
    };
    let socket = std::net::UdpSocket::bind(bind).ok()?;
    socket.connect(target).ok()?;
    let ip = socket.local_addr().ok()?.ip();
    (!ip.is_unspecified()).then_some(ip)
}

/// One look at the system.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Sample {
    pub routes: Routes,
    pub wall: SystemTime,
    pub mono: Instant,
    /// The DHT's (answers, timeouts) so far, with a DHT.
    pub dht: Option<(u64, u64)>,
}

/// What made the network count as back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Change {
    /// A local address came or changed.
    Address,
    /// A wake from sleep, with a route there.
    Wake,
    /// The DHT answers again after a stretch with none.
    DhtBack,
}

/// Turns samples into changes; no I/O, so its rules can be tested on their own.
#[derive(Default)]
pub(crate) struct Detector {
    last: Option<Sample>,
    // When the DHT last answered (the sample that saw its answer count grow), and its timeout
    // count then.
    dht_answered: Option<(Instant, u64)>,
}

impl Detector {
    pub fn observe(&mut self, now: Sample) -> Option<Change> {
        let Some(last) = self.last.replace(now) else {
            self.dht_answered = now.dht.map(|(_, timeouts)| (now.mono, timeouts));
            return None;
        };

        let dht_back = match (last.dht, now.dht) {
            (Some((answers_before, _)), Some((answers, timeouts))) if answers > answers_before => {
                let back = self.dht_answered.is_some_and(|(at, timeouts_then)| {
                    now.mono.duration_since(at) >= DHT_SILENT_WINDOW
                        && timeouts.saturating_sub(timeouts_then) >= DHT_SILENT_TIMEOUTS
                });
                self.dht_answered = Some((now.mono, timeouts));
                back
            }
            _ => false,
        };

        if now.routes.came_or_changed_since(&last.routes) {
            return Some(Change::Address);
        }
        // Slept: the peers' connections and waits are stale. Without a route yet (Wi-Fi still
        // connecting) the address coming is the change.
        let wall = now.wall.duration_since(last.wall).unwrap_or_default();
        if wall > POLL_INTERVAL + WAKE_GAP && now.routes.any() {
            return Some(Change::Wake);
        }
        if dht_back {
            return Some(Change::DhtBack);
        }
        None
    }
}

/// The session's watcher: polls until the session is gone. Holds the session only while it
/// acts, so a dropped session isn't kept alive by it.
pub(crate) async fn watch(session: Weak<Session>, bind_ip: Option<IpAddr>) -> anyhow::Result<()> {
    let mut detector = Detector::default();
    loop {
        let Some(s) = session.upgrade() else {
            return Ok(());
        };
        let sample = Sample {
            routes: Routes::read(bind_ip),
            wall: SystemTime::now(),
            mono: Instant::now(),
            dht: s.get_dht().map(|d| {
                let stats = d.stats();
                (stats.responses, stats.timeouts)
            }),
        };
        if let Some(change) = detector.observe(sample) {
            info!(?change, routes = ?sample.routes, "network came back or changed");
            s.network_changed();
        } else {
            debug!(routes = ?sample.routes, "network unchanged");
        }
        drop(s);
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A4: Ipv4Addr = Ipv4Addr::new(192, 168, 1, 20);
    const B4: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 7);

    fn v6(s: &str) -> Ipv6Addr {
        s.parse().unwrap()
    }

    /// Samples every POLL_INTERVAL from one start, the wall clock in step unless moved.
    struct Clock {
        mono: Instant,
        wall: SystemTime,
    }

    impl Clock {
        fn new() -> Self {
            Clock {
                mono: Instant::now(),
                wall: SystemTime::now(),
            }
        }
        fn tick(&mut self, routes: Routes, dht: Option<(u64, u64)>) -> Sample {
            self.mono += POLL_INTERVAL;
            self.wall += POLL_INTERVAL;
            Sample {
                routes,
                wall: self.wall,
                mono: self.mono,
                dht,
            }
        }
    }

    fn r4(v4: Option<Ipv4Addr>) -> Routes {
        Routes { v4, v6: None }
    }

    #[test]
    fn an_address_that_comes_back_or_changes_is_a_change() {
        let mut d = Detector::default();
        let mut c = Clock::new();
        assert_eq!(
            d.observe(c.tick(r4(Some(A4)), None)),
            None,
            "the first look"
        );
        assert_eq!(d.observe(c.tick(r4(Some(A4)), None)), None, "unchanged");
        assert_eq!(
            d.observe(c.tick(r4(None), None)),
            None,
            "lost: nothing to do yet"
        );
        assert_eq!(d.observe(c.tick(r4(None), None)), None);
        assert_eq!(
            d.observe(c.tick(r4(Some(A4)), None)),
            Some(Change::Address),
            "back"
        );
        assert_eq!(d.observe(c.tick(r4(Some(A4)), None)), None, "once");
        assert_eq!(
            d.observe(c.tick(r4(Some(B4)), None)),
            Some(Change::Address),
            "another network, or a VPN"
        );
    }

    #[test]
    fn an_ipv6_privacy_address_rotating_is_not_a_change() {
        let mut d = Detector::default();
        let mut c = Clock::new();
        let r = |s: &str| Routes {
            v4: Some(A4),
            v6: Some(v6(s)),
        };
        d.observe(c.tick(r("2001:db8:1:2::aaaa"), None));
        assert_eq!(d.observe(c.tick(r("2001:db8:1:2::bbbb"), None)), None);
        assert_eq!(
            d.observe(c.tick(r("2001:db8:9:9::bbbb"), None)),
            Some(Change::Address),
            "another /64: another network"
        );
        // IPv6 coming while IPv4 stays.
        let mut d = Detector::default();
        d.observe(c.tick(r4(Some(A4)), None));
        assert_eq!(
            d.observe(c.tick(r("2001:db8:1:2::1"), None)),
            Some(Change::Address)
        );
    }

    #[test]
    fn a_wake_from_sleep_is_a_change_once_there_is_a_route() {
        let mut d = Detector::default();
        let mut c = Clock::new();
        d.observe(c.tick(r4(Some(A4)), None));
        // An hour asleep: the monotonic clock may not count it, the wall clock does.
        c.wall += Duration::from_secs(3600);
        assert_eq!(d.observe(c.tick(r4(Some(A4)), None)), Some(Change::Wake));
        assert_eq!(d.observe(c.tick(r4(Some(A4)), None)), None, "once");

        // Woken before the Wi-Fi is back: the address coming is the change.
        c.wall += Duration::from_secs(600);
        assert_eq!(d.observe(c.tick(r4(None), None)), None);
        assert_eq!(d.observe(c.tick(r4(Some(A4)), None)), Some(Change::Address));

        // A poll a few seconds late (a busy machine) is not a wake.
        c.wall += WAKE_GAP / 2;
        assert_eq!(d.observe(c.tick(r4(Some(A4)), None)), None);
        // Nor the clock set back.
        c.wall -= Duration::from_secs(3600);
        assert_eq!(d.observe(c.tick(r4(Some(A4)), None)), None);
    }

    // The DHT's counts as a lookup sees them: a round of requests every minute, most answered,
    // the others timing out 10 s later.
    #[test]
    fn a_healthy_dht_is_never_a_change() {
        let mut d = Detector::default();
        let mut c = Clock::new();
        let (mut answers, mut timeouts) = (0, 0);
        for tick in 0..30 * 60 / 2 {
            match tick % 30 {
                0 => answers += 5,
                5 => timeouts += 6,
                _ => {}
            }
            let s = c.tick(r4(Some(A4)), Some((answers, timeouts)));
            assert_eq!(d.observe(s), None, "tick {tick}");
        }
    }

    #[test]
    fn the_dht_answering_again_after_an_outage_is_a_change() {
        let mut d = Detector::default();
        let mut c = Clock::new();
        let (mut answers, mut timeouts) = (10, 0);
        d.observe(c.tick(r4(Some(A4)), Some((answers, timeouts))));
        // Three minutes: rounds every minute, all timing out; the link stays up.
        for tick in 0..90 {
            if tick % 30 == 5 {
                timeouts += 8;
            }
            assert_eq!(
                d.observe(c.tick(r4(Some(A4)), Some((answers, timeouts)))),
                None
            );
        }
        answers += 3;
        assert_eq!(
            d.observe(c.tick(r4(Some(A4)), Some((answers, timeouts)))),
            Some(Change::DhtBack)
        );
        answers += 3;
        assert_eq!(
            d.observe(c.tick(r4(Some(A4)), Some((answers, timeouts)))),
            None,
            "once"
        );
    }

    #[test]
    fn a_dht_that_never_answers_is_never_a_change() {
        let mut d = Detector::default();
        let mut c = Clock::new();
        for tick in 0..600 {
            let timeouts = tick / 10;
            assert_eq!(d.observe(c.tick(r4(Some(A4)), Some((0, timeouts)))), None);
        }
    }

    // A machine with a network has a route; without one this reads none. Either way it reads
    // without failing, and a bound address of the other family gives that family nothing.
    #[test]
    fn routes_are_read_from_the_system() {
        let routes = Routes::read(None);
        debug!(?routes);
        assert_eq!(routes, Routes::read(None), "two reads in a row agree");
        let bound = Routes::read(Some(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert_eq!(bound.v6, None);
    }
}
