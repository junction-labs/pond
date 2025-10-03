mod metadata;
mod volume;

pub use metadata::{VolumeError, VolumeMetadata};
pub use volume::{AsyncFileReader, Error, Fd, Volume};

mod file;
pub use file::File;

pub mod read;

use std::{fmt, path::PathBuf, time::SystemTime};

use lasso::{Spur, ThreadedRodeo};
use once_cell::sync::Lazy;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    /// A regular file.
    Regular,
    /// A directory.
    Directory,
    /// A link to another file or directory.
    Symlink,
}

// NOTE:
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Ino {
    /// A missing or null ino. May be used to signal that a file is deleted or
    /// invalid.
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

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InternedString(Spur);

impl InternedString {
    pub fn new<S: AsRef<str>>(value: S) -> Self {
        Self(INTERNER.get_or_intern(value.as_ref()))
    }

    pub fn as_str(&self) -> &str {
        INTERNER.resolve(&self.0)
    }
}

impl AsRef<str> for InternedString {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl From<&str> for InternedString {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for InternedString {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<InternedString> for String {
    fn from(value: InternedString) -> Self {
        value.as_str().to_string()
    }
}

impl fmt::Debug for InternedString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for InternedString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

static INTERNER: Lazy<ThreadedRodeo<Spur>> = Lazy::new(ThreadedRodeo::new);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Location {
    Staged {
        path: PathBuf,
    },
    Local {
        path: PathBuf,
    },
    ObjectStorage {
        bucket: InternedString,
        key: InternedString,
    },
}

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
