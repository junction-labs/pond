use crate::{
    ByteRange, FileAttr, Ino, Location,
    file::File,
    read::{ChunkCache, ReadAheadPolicy},
    volume::{Volume, VolumeError},
};
use std::{
    collections::BTreeMap, io::SeekFrom, path::PathBuf, pin::Pin, sync::Arc, time::SystemTime,
};
use tempfile::TempDir;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWriteExt};

// TODO: We should use our own Path type abstraction here. std::fs::Path
// is pretty close to right but offers a bunch of system methods (like canonicalize)
// that mean it will do unexpected things if we use it as part of our interface.
//
// Both AWS and GCS commit to key names being valid utf8 but random filesystems we
// deal with won't enforce that. Since we're hiding object paths we have to figure
// out what we want to do about other encodings. Path/PathBuf are OsString under the
// hood which is right if we want to support that.

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
#[error("something went wrong")]
pub struct Error {
    kind: ErrorKind,
}

impl<E> From<E> for Error
where
    E: Into<ErrorKind>,
{
    fn from(err: E) -> Self {
        let kind = err.into();
        Self { kind }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ErrorKind {
    #[error("http request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("not found")]
    NotFound,

    #[error("volume error: {0}")]
    Volume(#[from] crate::volume::VolumeError),

    #[error("open error: {0}")]
    Open(#[from] std::io::Error),

    #[error("object storage error: {0}")]
    ObjectStore(#[from] object_store::Error),
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Fd(u64);

impl From<u64> for Fd {
    fn from(n: u64) -> Self {
        Self(n)
    }
}

impl From<Fd> for u64 {
    fn from(value: Fd) -> Self {
        value.0
    }
}

impl Fd {
    fn add(&self, rhs: u64) -> Fd {
        Fd(self.0.checked_add(rhs).expect("BUG: fd overflow"))
    }
}

#[allow(unused)]
enum FileDescriptor {
    ReadOnly(Pin<Box<dyn AsyncFileReader>>),
    Staged {
        file: tokio::fs::File,
        path: std::path::PathBuf,
    },
    Static(std::io::Cursor<Arc<[u8]>>),
    Commit,
    ClearCache,
}

pub trait AsyncFileReader: AsyncRead + AsyncSeek {}
impl<T: AsyncRead + AsyncSeek> AsyncFileReader for T {}

pub struct Client {
    volume: Volume,
    volume_version: Arc<[u8]>,

    cache: Arc<ChunkCache>,
    tempdir: TempDir,
    fds: BTreeMap<Fd, FileDescriptor>,
}

// TODO: the reads and writes here are mostly read_at and write_at, which would
// be way better than the existing

impl Client {
    fn lookup_fd(&mut self, fd: Fd) -> Option<&mut FileDescriptor> {
        self.fds.get_mut(&fd)
    }
}

impl Client {
    pub fn new(volume: Volume, max_cache_size: u64, chunk_size: u64, readahead: u64) -> Self {
        let cache = Arc::new(ChunkCache::new(
            max_cache_size,
            chunk_size,
            ReadAheadPolicy { size: readahead },
        ));

        // FIXME
        let tempdir = TempDir::with_prefix("cfs-").unwrap();
        let volume_version = std::sync::Arc::from(format!("{:#x}", 0xBEEF).into_bytes().as_slice());

        Self {
            tempdir,
            volume,
            volume_version,
            cache,
            fds: Default::default(),
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(self.volume.to_bytes()?)
    }

    pub fn getattr(&self, ino: Ino) -> Result<&FileAttr> {
        match self.volume.getattr(ino) {
            Some(attr) => Ok(attr),
            None => Err(Error {
                kind: ErrorKind::NotFound,
            }),
        }
    }

    pub fn setattr(
        &mut self,
        ino: Ino,
        mtime: Option<SystemTime>,
        ctime: Option<SystemTime>,
    ) -> Result<&FileAttr> {
        Ok(self.volume.setattr(ino, mtime, ctime)?)
    }

    pub fn modify(
        &mut self,
        ino: Ino,
        location: Option<Location>,
        range: Option<ByteRange>,
    ) -> Result<()> {
        self.volume.modify(ino, location, range)?;
        Ok(())
    }

    pub fn lookup(&self, parent: Ino, name: &str) -> Result<Option<&FileAttr>> {
        let attr = self.volume.lookup(parent, name)?;
        Ok(attr)
    }

    pub fn mkdir(&mut self, parent: Ino, name: String) -> Result<&FileAttr> {
        Ok(self.volume.mkdir(parent, name)?)
    }

    pub fn rmdir(&mut self, parent: Ino, name: &str) -> Result<()> {
        self.volume.rmdir(parent, name)?;
        Ok(())
    }

    pub fn readdir(&self, ino: Ino) -> Result<impl Iterator<Item = (&str, &FileAttr)>> {
        let iter = self.volume.readdir(ino)?;
        Ok(iter)
    }

    pub fn create(
        &mut self,
        parent: Ino,
        name: String,
        exclusive: bool,
    ) -> Result<(&FileAttr, Fd)> {
        let (path, f) = tempfile(self.tempdir.path());
        let attr = self.volume.create(
            parent,
            name,
            exclusive,
            Location::Staged { path: path.clone() },
            ByteRange::empty(),
        )?;

        let fd = new_fd(&mut self.fds, FileDescriptor::Staged { file: f, path });
        Ok((attr, fd))
    }

    pub fn delete(&mut self, parent: Ino, name: &str) -> Result<()> {
        self.volume.delete(parent, name)?;
        Ok(())
    }
}

impl Client {
    pub async fn open_read_write(&mut self, ino: Ino) -> Result<Fd> {
        match ino {
            Ino::COMMIT => {
                let fd = new_fd(&mut self.fds, FileDescriptor::Commit);
                Ok(fd)
            }
            Ino::VERSION => Err(Error::from(VolumeError::PermissionDenied)),
            ino => match self.volume.location(ino) {
                Some((Location::Staged { path }, _)) => {
                    let file = tokio::fs::File::open(path).await?;
                    let fd = new_fd(
                        &mut self.fds,
                        FileDescriptor::Staged {
                            file,
                            path: path.clone(),
                        },
                    );
                    Ok(fd)
                }
                Some(_) => Err(Error::from(VolumeError::PermissionDenied)),
                None => Err(Error::from(VolumeError::DoesNotExist)),
            },
        }
    }

    pub async fn open_read(&mut self, ino: Ino) -> Result<Fd> {
        match ino {
            Ino::VERSION => {
                let fd = new_fd(
                    &mut self.fds,
                    FileDescriptor::Static(std::io::Cursor::new(self.volume_version.clone())),
                );
                Ok(fd)
            }
            Ino::COMMIT => Err(Error::from(VolumeError::PermissionDenied)),
            ino => match self.volume.location(ino) {
                Some((l, range)) => {
                    let file = File::new(self.cache.clone(), l.clone(), *range);
                    let fd = new_fd(&mut self.fds, FileDescriptor::ReadOnly(Box::pin(file)));
                    Ok(fd)
                }
                None => Err(Error::from(VolumeError::DoesNotExist)),
            },
        }
    }

    pub async fn read_at(&mut self, fd: Fd, offset: u64, buf: &mut [u8]) -> Result<usize> {
        match self.lookup_fd(fd) {
            // reads of write-only special fds do nothing
            Some(FileDescriptor::ClearCache) | Some(FileDescriptor::Commit) => Ok(0),
            // static bytes just use their cursor
            Some(FileDescriptor::Static(cursor)) => {
                std::io::Seek::seek(cursor, SeekFrom::Start(offset))?;
                let n = std::io::Read::read(cursor, buf)?;
                Ok(n)
            }
            Some(FileDescriptor::ReadOnly(reader)) => {
                reader.seek(SeekFrom::Start(offset)).await?;
                let n = reader.read(buf).await?;
                Ok(n)
            }
            Some(FileDescriptor::Staged { file, .. }) => {
                // TODO: find a way to do File::read_at here instead of
                // a seek and a read as separate async ops
                file.seek(SeekFrom::Start(offset)).await?;
                let n = file.read(buf).await?;
                Ok(n)
            }
            None => Err(Error::from(VolumeError::DoesNotExist)),
        }
    }

    pub async fn write_at(&mut self, fd: Fd, offset: u64, data: &[u8]) -> Result<usize> {
        match self.lookup_fd(fd) {
            Some(FileDescriptor::ClearCache) => todo!(),
            Some(FileDescriptor::Commit) => {
                self.commit().await?;
                Ok(data.len())
            }
            // write directly into a staged file
            Some(FileDescriptor::Staged { file, .. }) => {
                file.seek(SeekFrom::Start(offset)).await?;
                let n = file.write(data).await?;
                Ok(n)
            }
            // no other fds are writable
            Some(_) => Err(Error::from(VolumeError::PermissionDenied)),
            None => Err(Error::from(VolumeError::DoesNotExist)),
        }
    }
}

// FIXME: this needs to allocate and check for remaining fds instead of just
// trying to increment every time and crashing. it's u64 so we probably won't
// hit it for a while but that's jank
fn new_fd(fd_set: &mut BTreeMap<Fd, FileDescriptor>, d: FileDescriptor) -> Fd {
    let next_fd = fd_set.keys().cloned().max().unwrap_or_default().add(1);
    fd_set.insert(next_fd, d);
    next_fd
}

// FIXME: handle errors
fn tempfile(tempdir: &std::path::Path) -> (PathBuf, tokio::fs::File) {
    let f = tempfile::Builder::new()
        .disable_cleanup(true)
        .tempfile_in(tempdir)
        .unwrap();

    let (f, path) = f.into_parts();
    (path.to_path_buf(), tokio::fs::File::from_std(f))
}

impl Client {
    async fn commit(&mut self) -> Result<()> {
        // - pick a new `Location` for the data, should be relative to the volume
        // - stage a multipart upload, collecting all of the new locations as
        //   we go. should be able to parallelize this step, optimize by chunking
        //   files together, but don't have to do that up front. save the location
        //   list while putting together the upload.
        // - once the data is written, modify the volume:
        //   - update the version
        //   - relocate individual files with volume.modify
        //   - write and upload the new volume metadata
        todo!()
    }
}
