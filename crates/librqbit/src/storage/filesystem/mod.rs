mod fs;
pub mod open_files;
mod opened_file;
mod sparse;

pub use fs::{FilesystemStorage, FilesystemStorageFactory};
pub use opened_file::OurFileExt;
#[cfg(windows)]
pub use opened_file::into_read_only;
