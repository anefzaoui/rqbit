use std::{
    fs::File,
    io::IoSlice,
    ops::Deref,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::Context;
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::Error;

use super::{
    fs::is_sharing_violation,
    open_files::{CachedHandle, Entry, OpenFiles},
};

pub trait OurFileExt {
    fn pwrite_all_vectored(&self, offset: u64, bufs: [IoSlice<'_>; 2]) -> anyhow::Result<usize>;
    fn pread_exact(&self, offset: u64, buf: &mut [u8]) -> anyhow::Result<()>;
    fn pwrite_all(&self, offset: u64, buf: &[u8]) -> anyhow::Result<()>;
}

impl OurFileExt for File {
    #[cfg(unix)]
    fn pwrite_all_vectored(&self, offset: u64, bufs: [IoSlice<'_>; 2]) -> anyhow::Result<usize> {
        // off_t is 32 bits on 32-bit Android (and on 32-bit Linux built without
        // _FILE_OFFSET_BITS=64), so pwritev cannot address anything past 2 GiB there.
        // pwrite_all goes through pwrite64 and takes the u64 as is, so past that point
        // write the two buffers one after the other instead.
        match nix::libc::off_t::try_from(offset) {
            Ok(offset) => {
                nix::sys::uio::pwritev(self, &bufs, offset).context("error calling pwritev")
            }
            Err(_) => pwrite_all_unvectored(self, offset, bufs),
        }
    }

    #[cfg(not(unix))]
    fn pwrite_all_vectored(&self, offset: u64, bufs: [IoSlice<'_>; 2]) -> anyhow::Result<usize> {
        match (bufs[0].len(), bufs[1].len()) {
            (len, 0) if len > 0 => {
                self.pwrite_all(offset, &bufs[0])?;
                Ok(len)
            }
            (0, len) if len > 0 => {
                self.pwrite_all(offset, &bufs[1])?;
                Ok(len)
            }
            (0, 0) => Ok(0),
            (l0, l1) => {
                // concatenate the buffers in memory so that we issue one write call instead of 2
                // assumes the message is <= CHUNK_SIZE
                use librqbit_core::constants::CHUNK_SIZE;
                let mut buf = [0u8; CHUNK_SIZE as usize];

                buf.get_mut(..l0)
                    .context("buf too small")?
                    .copy_from_slice(&bufs[0]);
                buf.get_mut(l0..l0 + l1)
                    .context("buf too small")?
                    .copy_from_slice(&bufs[1]);
                self.pwrite_all(offset, &buf[..l0 + l1])?;
                Ok(l0 + l1)
            }
        }
    }

    #[cfg(unix)]
    fn pread_exact(&self, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        use std::os::unix::fs::FileExt;

        Ok(self.read_exact_at(buf, offset)?)
    }

    #[cfg(windows)]
    fn pread_exact(&self, mut offset: u64, mut buf: &mut [u8]) -> anyhow::Result<()> {
        use std::os::windows::fs::FileExt;
        while !buf.is_empty() {
            let n = self.seek_read(buf, offset)?;
            if n == 0 {
                return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof").into());
            }
            offset += n as u64;
            buf = &mut buf[n..];
        }
        Ok(())
    }

    #[cfg(not(any(windows, unix)))]
    fn pread_exact(&self, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        anyhow::bail!("pread_exact not implemented for your platform")
    }

    #[cfg(unix)]
    fn pwrite_all(&self, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        use std::os::unix::fs::FileExt;
        Ok(self.write_all_at(buf, offset)?)
    }

    #[cfg(windows)]
    fn pwrite_all(&self, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        use std::os::windows::fs::FileExt;

        let mut remaining = buf.len();
        let mut buf = buf;
        let mut offset = offset;
        while remaining > 0 {
            let written = self.seek_write(&buf[..remaining], offset)?;
            remaining -= written;
            offset += written as u64;
            buf = &buf[written..];
        }
        Ok(())
    }

    #[cfg(not(any(windows, unix)))]
    fn pwrite_all(&self, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        anyhow::bail!("pwrite_all not implemented for your platform")
    }
}

/// What pwrite_all_vectored() falls back to when the offset does not fit the platform's
/// pwritev: the same bytes, in two plain positional writes.
#[cfg(unix)]
fn pwrite_all_unvectored(
    file: &File,
    mut offset: u64,
    bufs: [IoSlice<'_>; 2],
) -> anyhow::Result<usize> {
    for buf in &bufs {
        file.pwrite_all(offset, buf)?;
        offset += buf.len() as u64;
    }
    Ok(bufs[0].len() + bufs[1].len())
}

/// What an [`OpenedFile`] holds. The path stays when the handle is closed, so that the file can
/// be opened again; an empty path is padding, or what [`OpenedFile::take_clone`] left behind,
/// which never opens anything.
#[derive(Default, Debug)]
struct OpenedFileLocked {
    path: PathBuf,
    fd: Option<File>,
    /// `fd` can only read: opened for a read, or a finished file on Windows.
    read_only: bool,
    /// Every piece of the file is ours ([`OpenedFile::mark_finished`]): it is only read from
    /// now on.
    finished: bool,
    /// The file is on disk: made by us, or found there. One that was and is gone was moved or
    /// deleted, and is never made again.
    exists: bool,
    /// Made with `create_new` (the torrent may not overwrite files).
    create_new: bool,
    /// The length `ensure_file_length` asked for before the file was made: given when it is.
    len_when_made: Option<u64>,
    #[cfg(windows)]
    tried_marking_sparse: bool,
}

/// What the budget of open files ([`OpenFiles`]) sees of a file.
#[derive(Debug)]
struct Shared {
    entry: Entry,
    file: RwLock<OpenedFileLocked>,
    /// Written through since it was last written to disk ([`OpenedFile::close`] does, opening
    /// it again if the budget closed it meanwhile).
    written: AtomicBool,
}

impl Drop for Shared {
    fn drop(&mut self) {
        if self.file.get_mut().fd.is_some() {
            self.entry.closed();
        }
    }
}

impl CachedHandle for Shared {
    fn last_used(&self) -> u64 {
        self.entry.last_used()
    }

    /// Closed by the budget: only when no read or write uses it. It isn't written to disk here
    /// (that would cost a sync for every file of a torrent of thousands): it stays `written`,
    /// and a pause writes it to disk, as it did when every file was held open.
    fn try_close(&self) -> bool {
        let Some(mut g) = self.file.try_write() else {
            return false;
        };
        if g.fd.is_none() {
            return false;
        }
        drop(g.fd.take());
        self.entry.closed();
        true
    }
}

/// A file of the torrent.
///
/// Nothing is opened, nor made, by the storage's `init`. A read opens the file read-only; the
/// first write opens it read/write, and makes it (and its folders) if it isn't there yet: a
/// file nothing is written to, such as an unticked one that shares no piece with a ticked one,
/// is never made. Handles are counted in a budget shared by the process ([`OpenFiles`]): the
/// least recently used one is closed to open another, and opened again on its next use.
///
/// Its handle is also closed when the torrent is paused ([`Self::close`]). Opened again, a file
/// that was there is never made again: a file moved or deleted meanwhile is an error rather than
/// a new empty file written into as though it held the pieces.
#[derive(Debug)]
pub(crate) struct OpenedFile {
    shared: Arc<Shared>,
}

impl OpenedFile {
    /// A file at `path`, not opened yet. `exists`: it is there now. `create_new`: when it has to
    /// be made, it must not be there (the torrent may not overwrite files).
    pub fn lazy(path: PathBuf, exists: bool, create_new: bool, budget: &'static OpenFiles) -> Self {
        Self::from_locked(
            OpenedFileLocked {
                path,
                exists,
                create_new,
                ..Default::default()
            },
            budget,
            false,
        )
    }

    pub fn new_dummy(budget: &'static OpenFiles) -> Self {
        Self::from_locked(Default::default(), budget, false)
    }

    fn from_locked(locked: OpenedFileLocked, budget: &'static OpenFiles, written: bool) -> Self {
        let open = locked.fd.is_some();
        let shared = Arc::new(Shared {
            entry: Entry::new(budget),
            file: RwLock::new(locked),
            written: AtomicBool::new(written),
        });
        if open {
            shared.entry.opened(&shared);
        }
        Self { shared }
    }

    /// Moves the file, handle and all, to a new OpenedFile and leaves this one useless: it
    /// never opens the file again. This is how a torrent's files go from one state to the next.
    pub fn take_clone(&self) -> anyhow::Result<Self> {
        let mut g = self.shared.file.write();
        let f = std::mem::take(&mut *g);
        if f.fd.is_some() {
            self.shared.entry.closed();
        }
        drop(g);
        Ok(Self::from_locked(
            f,
            self.shared.entry.budget(),
            self.shared.written.swap(false, Ordering::AcqRel),
        ))
    }

    /// The handle to read with, opened (read-only) if there is none.
    pub fn for_read(&self) -> anyhow::Result<impl Deref<Target = File> + '_> {
        self.handle(false)
    }

    /// A handle that can write: opened again (read/write) if there is none or it is read-only;
    /// the file is made if it isn't there yet.
    pub fn for_write(&self) -> anyhow::Result<impl Deref<Target = File> + '_> {
        let f = self.handle(true)?;
        self.shared.written.store(true, Ordering::Release);
        Ok(f)
    }

    /// `ensure_file_length`: the file is given that length. One not made yet gets it when it is
    /// (its first write); an empty one is made now, as no write will ever come for it.
    pub fn ensure_len(&self, len: u64) -> anyhow::Result<()> {
        {
            let mut g = self.shared.file.write();
            if g.path.as_os_str().is_empty() {
                return Err(Error::FsFileIsNone.into());
            }
            if !g.exists && len > 0 {
                g.len_when_made = Some(len);
                return Ok(());
            }
            // As long already (a torrent resumed, or seeded): nothing to open or write.
            if g.exists && std::fs::metadata(&g.path).is_ok_and(|m| m.is_file() && m.len() == len) {
                return Ok(());
            }
        }
        Ok(self.for_write()?.set_len(len)?)
    }

    /// Writes what was written through the handle to disk and closes it (the torrent is
    /// paused). The next read or write opens the file again. A file the budget closed after it
    /// was written is opened again for this, by its path: written to disk before the bitfield
    /// that counts its pieces, as when every file was held open. One gone since has nothing of
    /// ours left to write.
    pub fn close(&self) -> anyhow::Result<()> {
        let mut g = self.shared.file.write();
        // Taking the lock for writing waited for every read and write still using the handle.
        let fd = g.fd.take();
        if fd.is_some() {
            self.shared.entry.closed();
        }
        let path = g.path.clone();
        drop(g);
        if !self.shared.written.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        let fd = match fd {
            Some(fd) => fd,
            None if path.as_os_str().is_empty() => return Ok(()),
            // Writing a file to disk takes a handle that can write, on Windows.
            None => match std::fs::OpenOptions::new()
                .read(true)
                .write(cfg!(windows))
                .open(&path)
            {
                Ok(fd) => fd,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(e) => {
                    self.shared.written.store(true, Ordering::Release);
                    return Err(e)
                        .with_context(|| format!("error opening {path:?} to write it to disk"));
                }
            },
        };
        fd.sync_data()
            .with_context(|| format!("error writing {path:?} to disk"))?;
        Ok(())
    }

    /// Every piece of the file is ours and verified: it is only read from now on.
    ///
    /// On Windows a handle that can write keeps other programs out of the file: one can't run
    /// it, and any program that opens it without sharing writing (most do, copying or reading it)
    /// gets a sharing violation. So there the handle becomes a read-only one of the same file,
    /// with std's sharing (read, write, delete): others can read, run, rename, move and delete it
    /// while it is seeded. Sharing writing too keeps whoever opens it for writing able to (MDM's
    /// own app does, checking a seeded torrent's files again when one is ticked). Elsewhere an
    /// open file stops none of that, and the handle stays as it is.
    pub fn mark_finished(&self) -> anyhow::Result<()> {
        let mut g = self.shared.file.write();
        if g.path.as_os_str().is_empty() {
            return Ok(());
        }
        g.finished = true;
        #[cfg(windows)]
        if !g.read_only
            && let Some(fd) = g.fd.take()
        {
            if self.shared.written.swap(false, Ordering::AcqRel)
                && let Err(e) = fd.sync_data()
            {
                g.fd = Some(fd);
                return Err(e).with_context(|| format!("error writing {:?} to disk", g.path));
            }
            match into_read_only(fd) {
                Ok(read_only) => {
                    g.fd = Some(read_only);
                    g.read_only = true;
                }
                Err((fd, e)) => {
                    g.fd = Some(fd);
                    return Err(e).with_context(|| format!("error opening {:?} read-only", g.path));
                }
            }
        }
        Ok(())
    }

    /// Whether a handle is open now.
    #[cfg(test)]
    pub fn is_open(&self) -> bool {
        self.shared.file.read().fd.is_some()
    }

    /// The handle if it can do what is asked, otherwise one opened for it.
    fn handle(&self, write: bool) -> anyhow::Result<MappedRwLockReadGuard<'_, File>> {
        let s = &self.shared;
        s.entry.touch();
        let g = s.file.read();
        let g = if g.usable(write) {
            g
        } else {
            drop(g);
            let mut w = s.file.write();
            if !w.usable(write) {
                w.prepare(write, s)?;
            }
            RwLockWriteGuard::downgrade(w)
        };
        RwLockReadGuard::try_map(g, |f| f.fd.as_ref()).map_err(|_| Error::FsFileIsNone.into())
    }
}

impl OpenedFileLocked {
    fn usable(&self, write: bool) -> bool {
        if self.fd.is_none() {
            return false;
        }
        if !write {
            return true;
        }
        #[cfg(windows)]
        if !self.tried_marking_sparse {
            return false;
        }
        !self.read_only
    }

    /// Makes the handle fit for what is asked, under the lock.
    fn prepare(&mut self, write: bool, shared: &Arc<Shared>) -> anyhow::Result<()> {
        if self.fd.is_none() || (write && self.read_only) {
            self.reopen(write, shared)?;
        }
        #[cfg(windows)]
        if write && !self.tried_marking_sparse {
            self.mark_sparse();
        }
        Ok(())
    }

    #[cfg(windows)]
    fn mark_sparse(&mut self) {
        self.tried_marking_sparse = true;
        if let Some(f) = self.fd.as_ref() {
            let marked = super::sparse::mark_file_sparse(f);
            tracing::debug!(path=?self.path, marked, "marking sparse");
        }
    }

    /// Opens the file: read-only for a read, read/write for a write. A file that was there is
    /// opened by its path, never made again; one never made is made by its first write, and a
    /// read of it is an error (its pieces are missing).
    fn reopen(&mut self, write: bool, shared: &Arc<Shared>) -> anyhow::Result<()> {
        if self.path.as_os_str().is_empty() {
            return Err(Error::FsFileIsNone.into());
        }
        // What this replaces, if anything, is a read-only handle: nothing to write to disk.
        if self.fd.take().is_some() {
            shared.entry.closed();
        }
        shared.entry.admit(shared);
        match self.open(write) {
            Ok(fd) => {
                self.fd = Some(fd);
                self.read_only = !write;
                Ok(())
            }
            Err(e) => {
                shared.entry.closed();
                Err(e)
            }
        }
    }

    /// [`Self::reopen`]'s open, counted in the budget already.
    fn open(&mut self, write: bool) -> anyhow::Result<File> {
        let read_only = !write;
        let fd = if self.exists {
            open_existing(&self.path, read_only)?
        } else if write {
            let fd = make(&self.path, self.create_new)?;
            self.exists = true;
            self.fd = Some(fd);
            #[cfg(windows)]
            self.mark_sparse();
            let fd = self.fd.take().expect("just set");
            if let Some(len) = self.len_when_made.take() {
                fd.set_len(len)
                    .with_context(|| format!("error setting the length of {:?}", self.path))?;
            }
            fd
        } else {
            match std::fs::OpenOptions::new().read(true).open(&self.path) {
                Ok(fd) => {
                    // Put there by someone else since: it is the file from now on.
                    self.exists = true;
                    fd
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Err(e).with_context(|| {
                        format!("{:?} isn't there: none of it was downloaded yet", self.path)
                    });
                }
                Err(e) => return Err(e).with_context(|| format!("error opening {:?}", self.path)),
            }
        };
        Ok(fd)
    }
}

/// Makes a file of the torrent, its folders first, for its first write.
fn make(path: &Path, create_new: bool) -> anyhow::Result<File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("error making the folder of {path:?}"))?;
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true);
    if create_new {
        options.create_new(true);
    } else {
        options.create(true).truncate(false);
    }
    options.open(path).with_context(|| {
        if create_new {
            format!("error creating a new file (because allow_overwrite = false) {path:?}")
        } else {
            format!("error opening {path:?} in read/write mode")
        }
    })
}

/// Opens a file that was there, without making it again.
fn open_existing(path: &Path, read_only: bool) -> anyhow::Result<File> {
    match std::fs::OpenOptions::new()
        .read(true)
        .write(!read_only)
        .open(path)
    {
        Ok(f) => Ok(f),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Err(e).with_context(|| format!("{path:?} is gone: it was moved or deleted"))
        }
        Err(e) if !read_only && is_sharing_violation(&e) => {
            Err(e).with_context(|| format!("{path:?} can't be written: another program holds it"))
        }
        Err(e) => Err(e).with_context(|| format!("error opening {path:?} again")),
    }
}

/// Windows: a read-only handle to the same file as `file`, which is closed (on error it is given
/// back), shared as std's handles are (read, write, delete): other programs may read, run,
/// rename, move and delete the file while it is held.
///
/// It is opened from the handle (`ReOpenFile`), not by name: it is the same file even if the
/// name was changed or replaced meanwhile.
#[cfg(windows)]
#[allow(unsafe_code)]
pub fn into_read_only(file: File) -> Result<File, (File, std::io::Error)> {
    use std::os::windows::io::{AsRawHandle, FromRawHandle};
    use windows::Win32::{
        Foundation::HANDLE,
        Storage::FileSystem::{
            FILE_FLAGS_AND_ATTRIBUTES, FILE_GENERIC_READ, FILE_SHARE_DELETE, FILE_SHARE_READ,
            FILE_SHARE_WRITE, ReOpenFile,
        },
    };
    // SAFETY: `file`'s handle is open for the whole call. ReOpenFile returns a new handle, which
    // only the File made here owns and closes.
    let reopened = unsafe {
        ReOpenFile(
            HANDLE(file.as_raw_handle()),
            FILE_GENERIC_READ.0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            FILE_FLAGS_AND_ATTRIBUTES(0),
        )
        .map(|h| File::from_raw_handle(h.0))
    };
    match reopened {
        Ok(read_only) => Ok(read_only),
        Err(e) => Err((file, e.into())),
    }
}

#[cfg(test)]
mod tests {
    use std::io::{IoSlice, Read};

    use librqbit_core::constants::CHUNK_SIZE;
    use peer_binary_protocol::DoubleBufHelper;
    use tempfile::TempDir;

    use crate::storage::filesystem::{open_files::OpenFiles, opened_file::OurFileExt};

    use super::OpenedFile;
    use crate::tests::test_util::held;

    /// A budget of this test's own: other tests' files never close these.
    fn budget(max: usize) -> &'static OpenFiles {
        Box::leak(Box::new(OpenFiles::new(max)))
    }

    fn opened(dir: &TempDir, name: &str, bytes: &[u8]) -> (std::path::PathBuf, OpenedFile) {
        let path = dir.path().join(name);
        let file = OpenedFile::lazy(path.clone(), false, true, budget(64));
        file.for_write().unwrap().pwrite_all(0, bytes).unwrap();
        (path, file)
    }

    fn read(file: &OpenedFile, len: usize) -> Vec<u8> {
        let mut buf = vec![0u8; len];
        file.for_read().unwrap().pread_exact(0, &mut buf).unwrap();
        buf
    }

    #[test]
    fn test_a_file_is_made_by_its_first_write_only() {
        let td = TempDir::with_prefix("test_made_by_first_write").unwrap();
        let path = td.path().join("sub").join("dir").join("a");
        let file = OpenedFile::lazy(path.clone(), false, false, budget(64));
        // Read: an error, and nothing made, not even its folder.
        let e = format!("{:#}", file.for_read().err().unwrap());
        assert!(e.contains("none of it was downloaded"), "{e}");
        assert!(!td.path().join("sub").exists());
        // Its length asked for: still nothing made.
        file.ensure_len(10).unwrap();
        assert!(!path.exists());
        // The first write makes it, with its folders and that length.
        file.for_write().unwrap().pwrite_all(2, b"hi").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"\0\0hi\0\0\0\0\0\0");
        file.close().unwrap();
    }

    #[test]
    fn test_a_file_as_long_already_is_not_written_for_its_length() {
        let td = TempDir::with_prefix("test_as_long_already").unwrap();
        let path = td.path().join("a");
        std::fs::write(&path, b"hello").unwrap();
        let file = OpenedFile::lazy(path.clone(), true, false, budget(64));
        file.ensure_len(5).unwrap();
        assert!(!file.is_open(), "nothing opened");
        assert!(!file.shared.written.load(super::Ordering::Acquire));
        file.ensure_len(8).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 8);
    }

    #[test]
    fn test_an_empty_file_is_made_when_its_length_is_asked() {
        let td = TempDir::with_prefix("test_empty_made").unwrap();
        let path = td.path().join("empty");
        let file = OpenedFile::lazy(path.clone(), false, false, budget(64));
        file.ensure_len(0).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    }

    #[test]
    fn test_reads_open_read_only_and_a_write_reopens() {
        let td = TempDir::with_prefix("test_reads_read_only").unwrap();
        let path = td.path().join("a");
        std::fs::write(&path, b"hello").unwrap();
        let file = OpenedFile::lazy(path.clone(), true, false, budget(64));
        assert!(!file.is_open(), "nothing opened before it is used");
        assert_eq!(read(&file, 5), b"hello");
        assert!(file.shared.file.read().read_only);
        assert!(file.for_read().unwrap().pwrite_all(0, b"x").is_err());
        file.for_write().unwrap().pwrite_all(0, b"J").unwrap();
        file.close().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"Jello");
    }

    #[test]
    fn test_allow_overwrite_off_never_writes_into_a_file_put_there() {
        let td = TempDir::with_prefix("test_create_new").unwrap();
        let path = td.path().join("a");
        let file = OpenedFile::lazy(path.clone(), false, true, budget(64));
        std::fs::write(&path, b"theirs").unwrap();
        assert!(file.for_write().is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"theirs");
    }

    #[test]
    fn test_the_budget_closes_the_least_recently_used_and_a_pause_syncs_it() {
        let td = TempDir::with_prefix("test_budget_closes").unwrap();
        let budget = budget(4);
        let files: Vec<_> = (0..20)
            .map(|i| {
                let f = OpenedFile::lazy(td.path().join(i.to_string()), false, false, budget);
                f.for_write()
                    .unwrap()
                    .pwrite_all(0, i.to_string().as_bytes())
                    .unwrap();
                assert!(budget.open_count() <= 4, "{}", budget.open_count());
                f
            })
            .collect();
        assert_eq!(files.iter().filter(|f| f.is_open()).count(), 4);
        assert!(
            files[16..].iter().all(|f| f.is_open()),
            "the latest stay open"
        );
        // Closed by the budget, still to be written to disk: a pause does it, opening each
        // again for that, and leaves it closed.
        let written = |f: &OpenedFile| f.shared.written.load(super::Ordering::Acquire);
        assert!(files[..16].iter().all(|f| written(f) && !f.is_open()));
        for f in &files {
            f.close().unwrap();
            assert!(!written(f) && !f.is_open());
        }
        assert_eq!(budget.open_count(), 0);
        // And each opens again where it is, with its bytes.
        for (i, f) in files.iter().enumerate() {
            let want = i.to_string();
            assert_eq!(read(f, want.len()), want.as_bytes());
            assert!(budget.open_count() <= 4);
        }
        drop(files);
        assert_eq!(budget.open_count(), 0, "dropped files leave the budget");
    }

    #[test]
    fn test_a_file_in_use_is_not_closed_by_the_budget() {
        let td = TempDir::with_prefix("test_in_use").unwrap();
        let budget = budget(1);
        let a = OpenedFile::lazy(td.path().join("a"), false, false, budget);
        let b = OpenedFile::lazy(td.path().join("b"), false, false, budget);
        let held_a = a.for_write().unwrap();
        held_a.pwrite_all(0, b"a").unwrap();
        // b needs a handle while a's is in use: b goes over the budget, a keeps its handle.
        b.for_write().unwrap().pwrite_all(0, b"b").unwrap();
        held_a.pwrite_all(1, b"a").unwrap();
        drop(held_a);
        assert!(a.is_open() && b.is_open());
        assert_eq!(budget.open_count(), 2);
    }

    #[test]
    fn test_close_releases_the_file_and_a_read_opens_it_again() {
        let td = TempDir::with_prefix("test_close_releases").unwrap();
        let (path, file) = opened(&td, "a", b"hello");
        assert_ne!(held(&path), Some(false), "open before closing");
        file.close().unwrap();
        assert!(!file.is_open());
        assert_ne!(held(&path), Some(true), "a closed file holds no handle");
        // Closed, the file can be renamed and renamed back by anyone, on every system.
        let moved = td.path().join("a-moved");
        std::fs::rename(&path, &moved).unwrap();
        std::fs::rename(&moved, &path).unwrap();
        assert_eq!(read(&file, 5), b"hello");
        assert!(file.is_open());
        file.for_write().unwrap().pwrite_all(5, b"!").unwrap();
        file.close().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"hello!");
    }

    #[test]
    fn test_a_taken_file_never_opens_again() {
        let td = TempDir::with_prefix("test_a_taken_file").unwrap();
        let (_path, file) = opened(&td, "a", b"hello");
        let taken = file.take_clone().unwrap();
        // A late write of the state the file was taken from must not get a handle of its own.
        assert!(file.for_write().is_err());
        assert!(file.for_read().is_err());
        assert!(!file.is_open());
        assert_eq!(read(&taken, 5), b"hello");
        // What was written before the move is still written to disk when the new one closes.
        assert!(taken.shared.written.load(super::Ordering::Acquire));
    }

    #[test]
    fn test_a_file_gone_while_closed_is_an_error_not_a_new_file() {
        let td = TempDir::with_prefix("test_a_file_gone").unwrap();
        let (path, file) = opened(&td, "a", b"hello");
        file.close().unwrap();
        std::fs::remove_file(&path).unwrap();
        let e = format!("{:#}", file.for_write().err().unwrap());
        assert!(e.contains("moved or deleted"), "{e}");
        let e = format!("{:#}", file.for_read().err().unwrap());
        assert!(e.contains("moved or deleted"), "{e}");
        assert!(!path.exists(), "a file gone is not made again");
    }

    // Windows: a finished file is held read-only and shared, so other programs can read it
    // without sharing writing, rename it and delete it, and it is still seeded from.
    #[cfg(windows)]
    #[test]
    fn test_a_finished_file_is_read_only_and_shared() {
        use std::os::windows::fs::OpenOptionsExt;
        let td = TempDir::with_prefix("test_a_finished_file").unwrap();
        let (path, file) = opened(&td, "a.exe", b"hello");
        // Before: our handle can write, and a reader that doesn't share writing is refused.
        let reader = || {
            std::fs::OpenOptions::new()
                .read(true)
                .share_mode(0x1) // FILE_SHARE_READ
                .open(&path)
        };
        assert!(reader().is_err(), "a writer of ours keeps such readers out");
        file.mark_finished().unwrap();
        assert!(file.is_open(), "a finished file stays open, to be seeded");
        let mut other = String::new();
        std::io::Read::read_to_string(&mut reader().unwrap(), &mut other).unwrap();
        assert_eq!(other, "hello");
        // A program that opens it to write still can (MDM's app does, checking a seeded
        // torrent's files again): the read-only handle shares writing, as std's do.
        drop(
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap(),
        );
        let moved = td.path().join("b.exe");
        std::fs::rename(&path, &moved).unwrap();
        assert_eq!(read(&file, 5), b"hello", "the handle follows the file");
        std::fs::remove_file(&moved).unwrap();
        assert!(!moved.exists());
    }

    // Windows: closed and opened again, a finished file is opened read-only; a write still
    // gets a handle that can write.
    #[cfg(windows)]
    #[test]
    fn test_a_finished_file_opened_again_is_read_only() {
        let td = TempDir::with_prefix("test_finished_again").unwrap();
        let (path, file) = opened(&td, "a", b"hello");
        file.mark_finished().unwrap();
        file.close().unwrap();
        assert_eq!(held(&path), Some(false));
        assert_eq!(read(&file, 5), b"hello");
        let moved = td.path().join("b");
        std::fs::rename(&path, &moved).unwrap();
        std::fs::rename(&moved, &path).unwrap();
        file.for_write().unwrap().pwrite_all(0, b"J").unwrap();
        file.close().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"Jello");
    }

    // The read-only handle into_read_only() gives is the same file, even under another name.
    #[cfg(windows)]
    #[test]
    fn test_into_read_only_keeps_the_same_file() {
        let td = TempDir::with_prefix("test_into_read_only").unwrap();
        let path = td.path().join("a");
        std::fs::write(&path, b"hello").unwrap();
        let rw = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let moved = td.path().join("b");
        std::fs::rename(&path, &moved).unwrap();
        std::fs::write(&path, b"other").unwrap();
        let ro = super::into_read_only(rw).map_err(|(_, e)| e).unwrap();
        let mut buf = [0u8; 5];
        ro.pread_exact(0, &mut buf).unwrap();
        assert_eq!(&buf, b"hello");
        assert!(ro.pwrite_all(0, b"x").is_err(), "read-only");
    }

    // Writes every split of a random buffer at `offset` with `write`, and checks the file
    // holds exactly those bytes there.
    fn check_pwrite_all_vectored(
        name: &str,
        offset: u64,
        write: impl Fn(&std::fs::File, u64, [IoSlice<'_>; 2]) -> anyhow::Result<usize>,
    ) {
        use std::io::Seek;

        let td = TempDir::with_prefix(name).unwrap();
        let mut tmp_buf = [0u8; CHUNK_SIZE as usize];
        for bufsize in [10000usize, CHUNK_SIZE as usize] {
            let mut buf = vec![0u8; bufsize];
            rand::fill(&mut buf[..]);
            for split_point in [0, bufsize / 2, bufsize] {
                let path = td.path().join(format!("file_{bufsize}_{split_point}"));
                let file = std::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&path)
                    .unwrap();
                let (first, second) = buf.split_at(split_point);
                let bufs = DoubleBufHelper::new(first, second).as_ioslices(bufsize);
                assert_eq!(write(&file, offset, bufs).unwrap(), bufsize, "{path:?}");

                let mut file = std::fs::File::open(&path).unwrap();
                assert_eq!(
                    file.metadata().unwrap().len(),
                    offset + bufsize as u64,
                    "{path:?}"
                );
                file.seek(std::io::SeekFrom::Start(offset)).unwrap();
                file.read_exact(&mut tmp_buf[..bufsize]).unwrap();
                assert_eq!(&tmp_buf[..bufsize], buf);
            }
        }
    }

    #[test]
    fn test_pwrite_all_vectored() {
        check_pwrite_all_vectored("test_pwrite_all_vectored", 0, |f, offset, bufs| {
            f.pwrite_all_vectored(offset, bufs)
        });
    }

    // 2 GiB is where a 32-bit off_t runs out. On a 64-bit host this still goes through
    // pwritev, so it cannot reproduce the 32-bit failure; on a 32-bit target the same
    // test takes the fallback.
    //
    // Unix only, and not because the code under test is: writing at this offset leaves a
    // 2 GiB hole, which is free where files are sparse by default and is not on NTFS,
    // where Windows zero-fills it -- six files of it, more than a CI runner has room or
    // time for. Windows takes the seek_write path below, whose offset is a u64 with no
    // such boundary, so there is nothing there for this test to find.
    #[cfg(unix)]
    #[test]
    fn test_pwrite_all_vectored_past_2gib() {
        check_pwrite_all_vectored(
            "test_pwrite_all_vectored_past_2gib",
            1u64 << 31,
            |f, offset, bufs| f.pwrite_all_vectored(offset, bufs),
        );
    }

    // The fallback itself, at an offset a 32-bit off_t cannot hold: the path a 32-bit
    // Android device takes for every write past 2 GiB into a file.
    #[cfg(unix)]
    #[test]
    fn test_pwrite_all_unvectored_past_2gib() {
        check_pwrite_all_vectored(
            "test_pwrite_all_unvectored_past_2gib",
            (1u64 << 31) + 12345,
            super::pwrite_all_unvectored,
        );
    }
}
