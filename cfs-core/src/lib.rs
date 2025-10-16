mod cache;
mod client;
mod error;
mod location;
mod metadata;
mod storage;
mod volume;

pub use client::Client;
pub use error::{Error, ErrorKind, Result};
pub use location::Location;
pub use metadata::{Modify, Version};
pub use volume::{Fd, Volume};

use std::time::SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    /// A regular file.
    Regular,
    /// A directory.
    Directory,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Ino {
    /// A missing or null ino. May be used to signal that a file is deleted or
    /// invalid.
    #[default]
    None,

    /// The root inode of a volume.
    Root,

    /// A reserved inode. Reserved inodes are used by the system when
    /// integrating with native filesystems.
    ///
    /// Reserved inode values are guaranteed to be in the range `[2, 15]`.
    Reserved(u64),

    /// A stable identifier for a file.
    Regular(u64),
}

impl PartialOrd for Ino {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Ino {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_u64().cmp(&other.as_u64())
    }
}

impl Ino {
    const NONE: u64 = 0;
    const ROOT: u64 = 1;
    const MIN_RESERVED: u64 = 2;
    const MIN_REGULAR: u64 = 32;

    pub const VERSION: Self = Ino::Reserved(2);
    pub const COMMIT: Self = Ino::Reserved(3);
    pub const CLEAR_CACHE: Self = Ino::Reserved(4);

    pub fn as_u64(&self) -> u64 {
        match self {
            Ino::None => Self::NONE,
            Ino::Root => Self::ROOT,
            Ino::Reserved(n) => *n,
            Ino::Regular(n) => *n,
        }
    }

    fn add(&self, n: u64) -> Ino {
        self.as_u64()
            .checked_add(n)
            .expect("BUG: ino overflowed u64")
            .into()
    }

    #[inline]
    fn is_root(&self) -> bool {
        matches!(self, Ino::Root)
    }

    #[inline]
    pub fn is_regular(&self) -> bool {
        matches!(self, Ino::Regular(_))
    }

    fn min_regular() -> Self {
        Ino::Regular(Self::MIN_REGULAR)
    }
}

impl From<Ino> for u64 {
    fn from(ino: Ino) -> Self {
        ino.as_u64()
    }
}

impl From<u64> for Ino {
    fn from(value: u64) -> Self {
        match value {
            Self::NONE => Ino::None,
            Self::ROOT => Ino::Root,
            Self::MIN_RESERVED..Self::MIN_REGULAR => Ino::Reserved(value),
            n => Ino::Regular(n),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FileAttr {
    /// Inode number
    pub ino: Ino,
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

// TODO: add checksums/etags here?

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteRange {
    pub offset: u64,
    pub len: u64,
}

impl ByteRange {
    pub fn empty() -> Self {
        Self { offset: 0, len: 0 }
    }

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
