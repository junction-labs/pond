mod flatbuffer;
mod volume;

use std::time::SystemTime;

#[derive(Debug, Clone, Copy)]
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
    /// Permissions
    pub perm: u16,
    /// User id
    pub uid: u32,
    /// Group id
    pub gid: u32,
}
