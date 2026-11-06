//! Which piece a peer is asked for next: in the torrent's order (the default, good for
//! playing a file while it downloads) or the rarest first (what most clients do: pieces few
//! peers have are fetched while those peers are there, and spread through the swarm sooner).

use std::sync::atomic::{AtomicU32, Ordering};

use librqbit_core::lengths::ValidPieceIndex;
use serde::{Deserialize, Serialize};

use crate::type_aliases::BF;

/// The order a torrent's pieces are downloaded in. Pieces a stream reader is waiting for
/// (`ManagedTorrent::stream`) always come first, whatever the order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PieceOrder {
    /// File by file (files sorted by name), each file's pieces from its start to its end.
    /// `first_and_last_first`: a file's first and last pieces come before the rest (a video's
    /// container often keeps its index at the end).
    InOrder { first_and_last_first: bool },
    /// The piece the fewest connected peers have, among those the asked peer has; ties broken
    /// in an order of this torrent's own, so that clients don't all ask for the same piece.
    RarestFirst,
}

impl Default for PieceOrder {
    fn default() -> Self {
        PieceOrder::InOrder {
            first_and_last_first: true,
        }
    }
}

/// How many live peers have each piece: kept up to date as peers' bitfields arrive, as they
/// announce pieces (HAVE) and as they leave, for [`PieceOrder::RarestFirst`].
#[derive(Default)]
pub(crate) struct PieceAvailability(Vec<AtomicU32>);

impl PieceAvailability {
    pub fn new(total_pieces: usize) -> Self {
        Self((0..total_pieces).map(|_| AtomicU32::new(0)).collect())
    }

    pub fn get(&self, piece: ValidPieceIndex) -> u32 {
        self.0
            .get(piece.get() as usize)
            .map_or(0, |c| c.load(Ordering::Relaxed))
    }

    pub fn inc(&self, piece: usize) {
        if let Some(c) = self.0.get(piece) {
            c.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn dec(&self, piece: usize) {
        if let Some(c) = self.0.get(piece) {
            // Never below zero, even if an update were missed.
            let _ = c.try_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_sub(1));
        }
    }

    /// A peer's pieces counted (a bitfield arrived) or uncounted (it left, or its bitfield was
    /// replaced).
    pub fn add(&self, bitfield: &BF) {
        for i in bitfield.iter_ones().take_while(|i| *i < self.0.len()) {
            self.inc(i);
        }
    }

    pub fn sub(&self, bitfield: &BF) {
        for i in bitfield.iter_ones().take_while(|i| *i < self.0.len()) {
            self.dec(i);
        }
    }
}

/// A piece's place among equally rare ones: a mix of its index and this torrent's own seed,
/// so different clients (and torrents) don't all prefer the same pieces.
pub(crate) fn tie_break(piece: ValidPieceIndex, seed: u64) -> u64 {
    let mut x = (piece.get() as u64) ^ seed;
    // splitmix64's finaliser.
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bf(bits: &[bool]) -> BF {
        let mut b = BF::from_boxed_slice(vec![0u8; bits.len().div_ceil(8)].into_boxed_slice());
        for (i, v) in bits.iter().enumerate() {
            b.set(i, *v);
        }
        b
    }

    #[test]
    fn counts_follow_peers() {
        let a = PieceAvailability::new(4);
        let lengths = librqbit_core::lengths::Lengths::new(4 * 16384, 16384).unwrap();
        let piece = |i| lengths.validate_piece_index(i).unwrap();
        a.add(&bf(&[true, true, false, false]));
        a.add(&bf(&[true, false, false, true]));
        a.inc(2);
        assert_eq!(
            (0..4).map(|i| a.get(piece(i))).collect::<Vec<_>>(),
            [2, 1, 1, 1]
        );
        a.sub(&bf(&[true, true, false, false]));
        assert_eq!(
            (0..4).map(|i| a.get(piece(i))).collect::<Vec<_>>(),
            [1, 0, 1, 1]
        );
        // Never below zero; bits past the torrent's end are ignored.
        a.sub(&bf(&[false, true, false, false, true, true, true, true]));
        assert_eq!(a.get(piece(1)), 0);
    }

    #[test]
    fn the_default_is_librqbits_order() {
        assert_eq!(
            PieceOrder::default(),
            PieceOrder::InOrder {
                first_and_last_first: true
            }
        );
    }
}
