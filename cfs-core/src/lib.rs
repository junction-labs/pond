pub mod file;
pub mod read;
pub mod volume;

use std::{path::PathBuf, time::SystemTime};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    /// A regular file.
    Regular,
    /// A directory.
    Directory,
    /// A link to another file or directory.
    Symlink,
}

#[derive(Debug, Clone)]
pub struct FileAttr {
    /// Inode number
    pub ino: u64,
    /// Size in bytes
    pub size: u64,
    /// Time of last modification
    pub mtime: SystemTime,
    /// Time of last change
    pub ctime: SystemTime,
    /// Kind of file (directory, file, pipe, etc)
    pub kind: FileType,
}

impl FileAttr {
    pub fn is_file(&self) -> bool {
        matches!(self.kind, FileType::Regular)
    }

    pub fn is_directory(&self) -> bool {
        matches!(self.kind, FileType::Directory)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Location {
    Local(PathBuf),
    ObjectStorage { bucket: String, key: String },
}

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub offset: u64,
    pub len: u64,
}

impl From<(u64, u64)> for ByteRange {
    fn from(val: (u64, u64)) -> Self {
        let (offset, len) = val;
        ByteRange { offset, len }
    }
}
