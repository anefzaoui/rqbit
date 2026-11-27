use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use anyhow::Context;
use buffers::ByteBufOwned;
use futures::{Stream, StreamExt, stream::FuturesUnordered};
use librqbit_core::torrent_metainfo::TorrentMetaV1Info;
use tracing::{Instrument, debug, debug_span};

use crate::{
    peer_connection::PeerConnectionOptions, peer_info_reader, spawn_utils::BlockingSpawner,
    stream_connect::StreamConnector,
};
use librqbit_core::hash_id::Id20;

/// How a magnet's metadata lookup is going (`AddTorrentOptions::metadata_progress`), for a client
/// that shows the wait: where peers came from, how many were asked, and how many answered. Every
/// count only grows.
#[derive(Debug, Default)]
pub struct MetadataProgress {
    /// Peer addresses the DHT gave (repeats included); with a `SessionOptions::peer_filter`,
    /// the ones it let through.
    pub from_dht: AtomicUsize,
    /// Peer addresses the trackers gave (repeats included); with a filter, the ones it let
    /// through.
    pub from_trackers: AtomicUsize,
    /// Peer addresses local service discovery gave (repeats included).
    pub from_lsd: AtomicUsize,
    /// Different peers asked for the metadata (the initial peers included).
    pub peers: AtomicUsize,
    /// Peers that answered the BitTorrent handshake.
    pub connected: AtomicUsize,
    /// Peers given up on: unreachable, closed the connection, or had no metadata to give.
    pub failed: AtomicUsize,
}

impl MetadataProgress {
    pub(crate) fn add(counter: &AtomicUsize) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum ReadMetainfoResult<Rx> {
    Found {
        info: TorrentMetaV1Info<ByteBufOwned>,
        info_bytes: ByteBufOwned,
        rx: Rx,
        seen: HashSet<SocketAddr>,
    },
    ChannelClosed {
        #[allow(dead_code)]
        seen: HashSet<SocketAddr>,
    },
}

/// Each address is asked once. `network` (the session's network changes,
/// `Session::network_changed`): when it changes, every address that failed is asked once more
/// at once: while the network was gone every one of them failed, and the DHT and the trackers
/// naming them again doesn't ask them again.
#[allow(clippy::too_many_arguments)]
pub async fn read_metainfo_from_peer_receiver<A: Stream<Item = SocketAddr> + Unpin>(
    peer_id: Id20,
    info_hash: Id20,
    initial_addrs: Vec<SocketAddr>,
    addrs_stream: A,
    peer_connection_options: Option<PeerConnectionOptions>,
    connector: Arc<StreamConnector>,
    client_name_and_version: String,
    progress: Option<Arc<MetadataProgress>>,
    mut network: Option<tokio::sync::watch::Receiver<u64>>,
) -> ReadMetainfoResult<A> {
    let mut seen = HashSet::<SocketAddr>::new();
    // The addresses whose last try failed, asked again when the network changes.
    let mut failed = HashSet::<SocketAddr>::new();
    let mut addrs = addrs_stream;

    let semaphore = tokio::sync::Semaphore::new(128);

    // `again`: an address asked before (after a network change): not counted again among the
    // peers asked and given up on, which count peers.
    let read_info_guarded = |addr, again: bool| {
        let semaphore = &semaphore;
        let connector = connector.clone();
        let client_name_and_version = client_name_and_version.clone();
        let progress = progress.clone();
        async move {
            let counted = progress.as_ref().filter(|_| !again);
            if let Some(p) = counted {
                MetadataProgress::add(&p.peers);
            }
            let token = match semaphore.acquire().await {
                Ok(token) => token,
                Err(e) => return (addr, Err(e.into())),
            };
            let ret = peer_info_reader::read_metainfo_from_peer(
                addr,
                peer_id,
                info_hash,
                peer_connection_options,
                // This shouldn't be called anyway as we aren't reading/writing to disk, so it's
                // ok not to use a shared one.
                BlockingSpawner::new(1),
                connector,
                client_name_and_version,
                progress.clone(),
            )
            .instrument(debug_span!("read_metainfo_from_peer", ?addr))
            .await
            .with_context(|| format!("error reading metainfo from {addr}"));
            drop(token);
            if let (Err(_), Some(p)) = (&ret, counted) {
                MetadataProgress::add(&p.failed);
            }
            (addr, ret)
        }
    };

    let mut unordered = FuturesUnordered::new();

    for a in initial_addrs {
        seen.insert(a);
        unordered.push(read_info_guarded(a, false));
    }

    let mut addrs_completed = false;

    loop {
        if addrs_completed && unordered.is_empty() {
            return ReadMetainfoResult::ChannelClosed { seen };
        }

        tokio::select! {
            done = unordered.next(), if !unordered.is_empty() => {
                match done {
                    Some((_, Ok((info, info_bytes)))) => return ReadMetainfoResult::Found { info, info_bytes, seen, rx: addrs },
                    Some((addr, Err(e))) => {
                        debug!("{:#}", e);
                        failed.insert(addr);
                    },
                    None => unreachable!()
                }
            }

            changed = async { network.as_mut().unwrap().changed().await }, if network.is_some() => {
                match changed {
                    Ok(()) => {
                        debug!(peers = failed.len(), "network changed, asking the peers that failed again");
                        for addr in failed.drain() {
                            unordered.push(read_info_guarded(addr, true));
                        }
                    }
                    // The session is gone.
                    Err(_) => network = None,
                }
            }

            next_addr = addrs.next(), if !addrs_completed => {
                match next_addr {
                    Some(addr) => {
                        if seen.insert(addr) {
                            unordered.push(read_info_guarded(addr, false));
                        }
                        continue;
                    },
                    None => {
                        addrs_completed = true;
                    },
                }
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use dht::{DhtBuilder, Id20};
    use librqbit_core::peer_id::generate_peer_id;

    use super::*;
    use std::{
        str::FromStr,
        sync::{Arc, Once},
    };

    static LOG_INIT: Once = Once::new();

    fn init_logging() {
        #[allow(unused_must_use)]
        LOG_INIT.call_once(|| {
            // pretty_env_logger::try_init();
        })
    }

    #[tokio::test]
    #[ignore]
    async fn read_metainfo_from_dht() {
        init_logging();

        let info_hash = Id20::from_str("cab507494d02ebb1178b38f2e9d7be299c86b862").unwrap();
        let dht = DhtBuilder::new().await.unwrap();

        let peer_rx = dht.get_peers(info_hash, None);
        let peer_id = generate_peer_id(b"-xx1234-");
        match read_metainfo_from_peer_receiver(
            peer_id,
            info_hash,
            Vec::new(),
            peer_rx,
            None,
            Arc::new(StreamConnector::new(Default::default()).await.unwrap()),
            crate::client_name_and_version().to_owned(),
            None,
            None,
        )
        .await
        {
            ReadMetainfoResult::Found { info, .. } => dbg!(info),
            ReadMetainfoResult::ChannelClosed { .. } => todo!("should not have happened"),
        };
    }
}
