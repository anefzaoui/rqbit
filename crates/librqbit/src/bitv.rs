use std::{path::PathBuf, sync::Arc};

use anyhow::Context;
use bitvec::{boxed::BitBox, order::Msb0, slice::BitSlice, vec::BitVec};
use parking_lot::Mutex;
use tracing::debug_span;

use crate::{spawn_utils::BlockingSpawner, storage::filesystem::OurFileExt};

pub trait BitV: Send + Sync {
    fn as_slice(&self) -> &BitSlice<u8, Msb0>;
    fn as_slice_mut(&mut self) -> &mut BitSlice<u8, Msb0>;
    fn into_dyn(self) -> Box<dyn BitV>;
    fn as_bytes(&self) -> &[u8];
    fn flush(&mut self, flush_async: bool) -> anyhow::Result<()>;
}

pub type BoxBitV = Box<dyn BitV>;

struct DiskFlushRequest {
    snapshot: BitBox<u8, Msb0>,
    generation: u64,
}

/// The .bitv file, shared by the flusher task (asynchronous flushes) and the caller of a
/// synchronous flush, which writes it itself so it is on disk when `flush` returns.
struct DiskWriter {
    filename: PathBuf,
    file: std::fs::File,
    /// The newest snapshot on disk (or skipped as older than it): a queued older snapshot
    /// never overwrites a newer one written synchronously in the meantime.
    written: u64,
    /// A write failed and the file was removed; nothing is written any more.
    failed: bool,
}

impl DiskWriter {
    fn write(&mut self, generation: u64, bytes: &[u8]) -> anyhow::Result<()> {
        if self.failed {
            anyhow::bail!("an earlier write to {:?} failed", self.filename);
        }
        if generation <= self.written {
            return Ok(());
        }
        if let Err(e) = self.file.pwrite_all(0, bytes) {
            self.failed = true;
            if let Err(e) = std::fs::remove_file(&self.filename) {
                tracing::error!(filename=?self.filename, "error removing bitv: {e:#}");
            }
            return Err(e).with_context(|| format!("error writing to {:?}", self.filename));
        }
        self.written = generation;
        self.file
            .sync_all()
            .with_context(|| format!("error fsyncing {:?}", self.filename))
    }
}

pub struct DiskBackedBitV {
    bv: BitBox<u8, Msb0>,
    flush_tx: tokio::sync::mpsc::UnboundedSender<DiskFlushRequest>,
    writer: Arc<Mutex<DiskWriter>>,
    spawner: BlockingSpawner,
    /// Numbers the snapshots flushed, for `DiskWriter::written`.
    generation: u64,
    /// Changed (`as_slice_mut`) since the last snapshot flushed.
    dirty: bool,
}

impl Drop for DiskBackedBitV {
    fn drop(&mut self) {
        self.generation += 1;
        if self
            .flush_tx
            .send(DiskFlushRequest {
                snapshot: self.bv.clone(),
                generation: self.generation,
            })
            .is_err()
        {
            tracing::warn!("error flushing bitv on drop: flusher task is dead")
        }
    }
}

// NOTE on mmap. rqbit used it for a while, but it has issues on slow disks.
// We want writes to bitv to be instant in RAM. However when disk is slow, occasionally
// the writes stall which blocks the executor.
// Thus this separate "thread" of flushing was implemented. A synchronous flush (on pause,
// when the torrent finishes) writes on the caller's thread instead, so the file is on disk
// when flush() returns.
impl DiskBackedBitV {
    pub async fn new(filename: PathBuf, spawner: BlockingSpawner) -> anyhow::Result<Self> {
        let buf = tokio::fs::read(&filename)
            .await
            .with_context(|| format!("error reading {filename:?}"))?;
        let bv = BitVec::from_vec(buf).into_boxed_bitslice();

        // blocking file to avoid double-buffering and double-memcpy
        let file = spawner
            .block_in_place_with_semaphore(|| {
                std::fs::OpenOptions::new()
                    .write(true)
                    .create(false)
                    .open(&filename)
            })
            .await
            .with_context(|| format!("error opening {filename:?}"))?;
        let writer = Arc::new(Mutex::new(DiskWriter {
            filename: filename.clone(),
            file,
            written: 0,
            failed: false,
        }));

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DiskFlushRequest>();
        let task_writer = writer.clone();
        let task_spawner = spawner.clone();
        librqbit_core::spawn_utils::spawn(
            debug_span!("diskbitv-flusher", ?filename),
            format!("DiskBackedBitV::flusher {filename:?}"),
            async move {
                loop {
                    let Some(mut req) = rx.recv().await else {
                        break;
                    };
                    while let Ok(r) = rx.try_recv() {
                        req = r;
                    }

                    let res = task_spawner
                        .block_in_place_with_semaphore(|| {
                            task_writer
                                .lock()
                                .write(req.generation, req.snapshot.as_raw_slice())
                        })
                        .await;
                    if let Err(e) = res {
                        tracing::error!(?filename, "error saving bitv: {e:#}");
                        if task_writer.lock().failed {
                            break;
                        }
                    }
                }

                Ok::<_, anyhow::Error>(())
            },
        );
        Ok(Self {
            bv,
            flush_tx: tx,
            writer,
            spawner,
            generation: 0,
            dirty: false,
        })
    }
}

#[async_trait::async_trait]
impl BitV for BitBox<u8, Msb0> {
    fn as_slice(&self) -> &BitSlice<u8, Msb0> {
        self.as_bitslice()
    }

    fn as_slice_mut(&mut self) -> &mut BitSlice<u8, Msb0> {
        self.as_mut_bitslice()
    }

    fn as_bytes(&self) -> &[u8] {
        self.as_raw_slice()
    }

    fn flush(&mut self, _flush_async: bool) -> anyhow::Result<()> {
        Ok(())
    }

    fn into_dyn(self) -> Box<dyn BitV> {
        Box::new(self)
    }
}

impl BitV for DiskBackedBitV {
    fn as_slice(&self) -> &BitSlice<u8, Msb0> {
        self.bv.as_bitslice()
    }

    fn as_slice_mut(&mut self) -> &mut BitSlice<u8, Msb0> {
        self.dirty = true;
        self.bv.as_mut_bitslice()
    }

    fn as_bytes(&self) -> &[u8] {
        self.bv.as_raw_slice()
    }

    fn flush(&mut self, flush_async: bool) -> anyhow::Result<()> {
        if flush_async {
            self.generation += 1;
            self.dirty = false;
            let req = DiskFlushRequest {
                snapshot: self.bv.clone(),
                generation: self.generation,
            };
            return self.flush_tx.send(req).context("flusher task is dead");
        }
        // On disk before returning: a quit right after a pause keeps the pieces. Unchanged
        // since a snapshot that is on disk already: nothing to write. Unchanged since one only
        // queued for the flusher: written now.
        if !self.dirty && self.writer.lock().written >= self.generation {
            return Ok(());
        }
        self.generation += 1;
        self.dirty = false;
        let generation = self.generation;
        let bytes = self.bv.as_raw_slice();
        let writer = &self.writer;
        self.spawner
            .block_in_place(|| writer.lock().write(generation, bytes))
    }

    fn into_dyn(self) -> Box<dyn BitV> {
        Box::new(self)
    }
}

impl BitV for Box<dyn BitV> {
    fn as_slice(&self) -> &BitSlice<u8, Msb0> {
        (**self).as_slice()
    }

    fn as_slice_mut(&mut self) -> &mut BitSlice<u8, Msb0> {
        (**self).as_slice_mut()
    }

    fn as_bytes(&self) -> &[u8] {
        (**self).as_bytes()
    }

    fn flush(&mut self, flush_async: bool) -> anyhow::Result<()> {
        (**self).flush(flush_async)
    }

    fn into_dyn(self) -> Box<dyn BitV> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A synchronous flush (pause, finish) is on disk when it returns, so a quit right after a
    // pause keeps the pieces. The test runtime has one thread: the flusher task can't run
    // before the file is read, so a flush that only queued the write fails here every time.
    #[tokio::test]
    async fn test_sync_flush_is_on_disk_when_it_returns() {
        let dir = tempfile::TempDir::with_prefix("rqbit_bitv").unwrap();
        let filename = dir.path().join("test.bitv");
        std::fs::write(&filename, [0u8; 2]).unwrap();
        let mut bv = DiskBackedBitV::new(filename.clone(), BlockingSpawner::new(1))
            .await
            .unwrap();

        bv.as_slice_mut().set(0, true);
        bv.as_slice_mut().set(9, true);
        bv.flush(false).unwrap();
        assert_eq!(
            std::fs::read(&filename).unwrap(),
            [0b1000_0000, 0b0100_0000]
        );

        bv.as_slice_mut().set(1, true);
        bv.flush(false).unwrap();
        assert_eq!(
            std::fs::read(&filename).unwrap(),
            [0b1100_0000, 0b0100_0000]
        );
        // Saved by an asynchronous flush (the periodic one) that is only queued: a synchronous
        // flush right after it, with nothing new, still writes it.
        bv.as_slice_mut().set(2, true);
        bv.flush(true).unwrap();
        bv.flush(false).unwrap();
        assert_eq!(
            std::fs::read(&filename).unwrap(),
            [0b1110_0000, 0b0100_0000]
        );
    }

    // A snapshot queued for the flusher before a synchronous flush never overwrites the newer
    // pieces that flush wrote.
    #[test]
    fn test_older_snapshot_does_not_overwrite_a_newer_one() {
        let dir = tempfile::TempDir::with_prefix("rqbit_bitv").unwrap();
        let filename = dir.path().join("test.bitv");
        std::fs::write(&filename, [0u8; 1]).unwrap();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&filename)
            .unwrap();
        let mut writer = DiskWriter {
            filename: filename.clone(),
            file,
            written: 0,
            failed: false,
        };

        writer.write(2, &[0b1100_0000]).unwrap();
        writer.write(1, &[0b1000_0000]).unwrap();
        assert_eq!(std::fs::read(&filename).unwrap(), [0b1100_0000]);

        writer.write(3, &[0b1110_0000]).unwrap();
        assert_eq!(std::fs::read(&filename).unwrap(), [0b1110_0000]);
    }
}
