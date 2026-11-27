//! A budget of open file handles, shared by every torrent of the process.
//!
//! A torrent of thousands of files used to hold a handle on each of them for as long as it was
//! live: past the system's limit (256 by default on macOS) adding it failed with "Too many open
//! files", and on Windows it held thousands of handles. Files are now opened when they are read
//! or written, and when the budget is used up the handle used least recently is closed; it is
//! opened again on its next use.
//!
//! A handle is only closed when nothing reads or writes through it ([`CachedHandle::try_close`]
//! never waits). If every handle is busy, an open goes over the budget rather than wait: that
//! can't be more than the reads and writes in flight. Closing one doesn't write it to disk (a
//! closed file's data is the system's to write, as an open one's is); what was written through
//! it is still written to disk when its torrent is paused, as before.
//!
//! Locks: a file's own lock may be held while the registry's is taken, never the other way
//! round (the registry only *tries* a file's lock, from outside its own).

use std::{
    collections::HashMap,
    sync::{
        Arc, OnceLock, Weak,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use parking_lot::Mutex;

/// A handle the budget may close.
pub trait CachedHandle: Send + Sync {
    /// When it was last used ([`OpenFiles::now`] at that use).
    fn last_used(&self) -> u64;
    /// Closes the handle if no read or write uses it now, without waiting, and tells the budget
    /// ([`OpenFiles::closed`]). It still knows it was written through, so that a pause writes it
    /// to disk. Whether it closed it.
    fn try_close(&self) -> bool;
}

/// How many handles may be open, which are, and a clock to tell which was used last.
pub struct OpenFiles {
    /// 0: [`default_max`], read again at each open, so that a process that raises its limit
    /// (`try_increase_nofile_limit`) gets more.
    max: AtomicUsize,
    clock: AtomicU64,
    ids: AtomicU64,
    open: Mutex<HashMap<u64, Weak<dyn CachedHandle>>>,
}

/// The budget every torrent of the process shares (see [`default_max`]).
pub fn global() -> &'static OpenFiles {
    static GLOBAL: OnceLock<OpenFiles> = OnceLock::new();
    GLOBAL.get_or_init(OpenFiles::following_the_system)
}

/// What the global budget allows unless told otherwise: a quarter of the process's limit on
/// open files on Unix (peers' sockets and everything else need the rest), between 16 and 1024:
/// macOS' default of 256 gives 64, Linux' 1024 gives 256. Windows has no such limit; 512 there.
pub fn default_max() -> usize {
    #[cfg(unix)]
    {
        use nix::sys::resource::{Resource, getrlimit};
        match getrlimit(Resource::RLIMIT_NOFILE) {
            Ok((soft, _)) => usize::try_from(soft / 4).unwrap_or(1024).clamp(16, 1024),
            Err(_) => 64,
        }
    }
    #[cfg(not(unix))]
    {
        512
    }
}

impl OpenFiles {
    pub fn new(max: usize) -> Self {
        Self::with(max.max(1))
    }

    /// A budget of [`default_max`], as the system's limit is at each open.
    pub fn following_the_system() -> Self {
        Self::with(0)
    }

    fn with(max: usize) -> Self {
        Self {
            max: AtomicUsize::new(max),
            clock: AtomicU64::new(1),
            ids: AtomicU64::new(1),
            open: Mutex::new(HashMap::new()),
        }
    }

    pub fn max(&self) -> usize {
        match self.max.load(Ordering::Relaxed) {
            0 => default_max(),
            max => max,
        }
    }

    /// A new budget; handles over it are closed as files are opened.
    pub fn set_max(&self, max: usize) {
        self.max.store(max.max(1), Ordering::Relaxed);
    }

    /// How many handles are open now.
    pub fn open_count(&self) -> usize {
        self.open.lock().len()
    }

    /// An id for a handle's entry.
    pub fn new_id(&self) -> u64 {
        self.ids.fetch_add(1, Ordering::Relaxed)
    }

    /// The time of a use, for [`CachedHandle::last_used`].
    pub fn now(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed)
    }

    /// Before a handle is opened (by `except`, which is never closed for it): closes the least
    /// recently used ones until there is room for it. Busy ones are passed over.
    pub fn make_room(&self, except: u64) {
        loop {
            // The handles are only upgraded under the lock, and dropped after it: the last
            // reference to one going away runs its drop, which tells the registry.
            let (excess, mut all) = {
                let mut open = self.open.lock();
                open.retain(|_, h| h.strong_count() > 0);
                let max = self.max();
                let mine = usize::from(open.contains_key(&except));
                // Room for one more (this one, unless it is counted already).
                let excess = (open.len() - mine + 1).saturating_sub(max);
                if excess == 0 {
                    return;
                }
                let all: Vec<Arc<dyn CachedHandle>> = open
                    .iter()
                    .filter(|(id, _)| **id != except)
                    .filter_map(|(_, h)| h.upgrade())
                    .collect();
                (excess, all)
            };
            // A few more than needed: some may be busy.
            let want = (excess + 4).min(all.len());
            if want == 0 {
                return;
            }
            let mut all: Vec<(u64, Arc<dyn CachedHandle>)> =
                all.drain(..).map(|h| (h.last_used(), h)).collect();
            if want < all.len() {
                all.select_nth_unstable_by_key(want - 1, |(t, _)| *t);
                all.truncate(want);
            }
            all.sort_unstable_by_key(|(t, _)| *t);
            let mut closed = 0;
            for (_, h) in all {
                if h.try_close() {
                    closed += 1;
                    if self.open_count() < self.max() {
                        return;
                    }
                }
            }
            if closed == 0 {
                // Every one of them is busy: over the budget for now.
                return;
            }
        }
    }

    /// A handle was opened (after [`Self::make_room`]).
    pub fn opened(&self, id: u64, handle: Weak<dyn CachedHandle>) {
        self.open.lock().insert(id, handle);
    }

    /// A handle was closed (or the file dropped).
    pub fn closed(&self, id: u64) {
        self.open.lock().remove(&id);
    }
}

/// What [`CachedHandle`]s keep to be in a budget: their entry's id, the budget, and when they
/// were last used.
pub struct Entry {
    id: u64,
    budget: &'static OpenFiles,
    last_used: AtomicU64,
}

impl std::fmt::Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry").field("id", &self.id).finish()
    }
}

impl Entry {
    pub fn new(budget: &'static OpenFiles) -> Self {
        Self {
            id: budget.new_id(),
            budget,
            last_used: AtomicU64::new(budget.now()),
        }
    }

    pub fn budget(&self) -> &'static OpenFiles {
        self.budget
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn touch(&self) {
        self.last_used.store(self.budget.now(), Ordering::Relaxed);
    }

    pub fn last_used(&self) -> u64 {
        self.last_used.load(Ordering::Relaxed)
    }

    /// Room made for this handle, before it is opened.
    pub fn make_room(&self) {
        self.budget.make_room(self.id);
    }

    pub fn opened<T: CachedHandle + 'static>(&self, me: &Arc<T>) {
        let weak: Weak<T> = Arc::downgrade(me);
        let weak: Weak<dyn CachedHandle> = weak;
        self.touch();
        self.budget.opened(self.id, weak);
    }

    pub fn closed(&self) {
        self.budget.closed(self.id);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use parking_lot::RwLock;

    use super::*;

    /// A handle that is "open" while its flag is set; `busy` holds its lock as a read would.
    struct Fake {
        entry: Entry,
        open: RwLock<bool>,
        closed_once: AtomicBool,
    }

    impl CachedHandle for Fake {
        fn last_used(&self) -> u64 {
            self.entry.last_used()
        }
        fn try_close(&self) -> bool {
            let Some(mut g) = self.open.try_write() else {
                return false;
            };
            if !*g {
                return false;
            }
            self.closed_once.store(true, Ordering::Relaxed);
            *g = false;
            self.entry.closed();
            true
        }
    }

    fn leak(max: usize) -> &'static OpenFiles {
        Box::leak(Box::new(OpenFiles::new(max)))
    }

    fn open(budget: &'static OpenFiles) -> Arc<Fake> {
        let f = Arc::new(Fake {
            entry: Entry::new(budget),
            open: RwLock::new(false),
            closed_once: AtomicBool::new(false),
        });
        use_it(&f);
        f
    }

    fn use_it(f: &Arc<Fake>) {
        if !*f.open.read() {
            f.entry.make_room();
            *f.open.write() = true;
            f.entry.opened(f);
        }
        f.entry.touch();
    }

    #[test]
    fn test_never_more_open_than_the_budget() {
        let budget = leak(8);
        let files: Vec<_> = (0..100).map(|_| open(budget)).collect();
        assert_eq!(budget.open_count(), 8);
        assert_eq!(files.iter().filter(|f| *f.open.read()).count(), 8);
        // The last ones opened are the ones still open; the others were closed.
        assert!(files[92..].iter().all(|f| *f.open.read()));
        assert!(files[..92].iter().all(|f| f.closed_once.load(Ordering::Relaxed)));
    }

    #[test]
    fn test_the_least_recently_used_is_closed_first() {
        let budget = leak(3);
        let a = open(budget);
        let b = open(budget);
        let c = open(budget);
        use_it(&a);
        let _d = open(budget);
        assert!(*a.open.read(), "used again: kept");
        assert!(!*b.open.read(), "used least recently: closed");
        assert!(*c.open.read());
    }

    #[test]
    fn test_a_busy_handle_is_never_closed() {
        let budget = leak(2);
        let a = open(budget);
        let b = open(budget);
        // A read in flight on both: the third open goes over the budget rather than wait.
        let ga = a.open.read();
        let gb = b.open.read();
        let c = open(budget);
        assert!(*ga && *gb && *c.open.read());
        assert_eq!(budget.open_count(), 3);
        drop((ga, gb));
        // Next open: back within it.
        let _d = open(budget);
        assert_eq!(budget.open_count(), 2);
    }

    #[test]
    fn test_a_dropped_handle_leaves_the_budget() {
        let budget = leak(2);
        let a = open(budget);
        drop(a);
        let _b = open(budget);
        let _c = open(budget);
        assert_eq!(budget.open_count(), 2);
    }

    #[test]
    fn test_lowering_the_budget_closes_on_the_next_open() {
        let budget = leak(10);
        let files: Vec<_> = (0..10).map(|_| open(budget)).collect();
        budget.set_max(4);
        let _e = open(budget);
        assert_eq!(budget.open_count(), 4);
        drop(files);
    }

    #[test]
    fn test_default_budget_is_sane() {
        let m = default_max();
        assert!((16..=1024).contains(&m), "{m}");
        assert_eq!(OpenFiles::following_the_system().max(), m);
        let pinned = OpenFiles::following_the_system();
        pinned.set_max(7);
        assert_eq!(pinned.max(), 7);
    }
}
