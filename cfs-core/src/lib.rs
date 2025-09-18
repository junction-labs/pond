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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteRange {
    pub offset: u64,
    pub len: u64,
}

impl ByteRange {
    pub fn contains(&self, idx: u64) -> bool {
        self.offset <= idx && idx < self.end()
    }

    /// Return the end of the range, where the byte at `end` is exclusive.
    #[inline]
    pub fn end(&self) -> u64 {
        self.offset + self.len
    }

    /// Advance offset by n bytes while keep the end of the the byte range the same.
    pub fn advance(&mut self, n: u64) {
        debug_assert!(self.len - n > 0);

        self.offset += n;
        self.len -= n;
    }

    pub fn as_range_usize(&self) -> std::ops::Range<usize> {
        std::ops::Range {
            start: self.offset as usize,
            end: self.end() as usize,
        }
    }

    pub fn as_range_u64(&self) -> std::ops::Range<u64> {
        std::ops::Range {
            start: self.offset,
            end: self.end(),
        }
    }
}

impl From<(u64, u64)> for ByteRange {
    fn from(val: (u64, u64)) -> Self {
        let (offset, len) = val;
        ByteRange { offset, len }
    }
}
