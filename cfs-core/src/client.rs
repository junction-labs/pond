use crate::{
    ByteRange, FileAttr, Ino, Location,
    file::File,
    read::{ChunkCache, ReadAheadPolicy},
    volume::Volume,
};
use std::{pin::Pin, sync::Arc, time::SystemTime};
use tokio::io::{AsyncRead, AsyncSeek};

// TODO: We're starting with a Reader/Writer split for modifying files in a volume
// to make it clear that you're either getting an input or output stream, not doing
// random writes. If that changes we should switch to something more like a File
// struct at write_at/pread(2) methods.

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

pub trait AsyncFileReader: AsyncRead + AsyncSeek {}
impl<T: AsyncRead + AsyncSeek> AsyncFileReader for T {}

pub struct Client {
    // TODO: this could be multiple volume versions?
    // so if we have the most recent version, and it isn't compacted yet, then we kind of need a
    // chained-volume lookup here. this might be as simple as a ptr to another Volume, which we use
    // as a fallback if current version doesn't have anything.
    volume: Volume,
    // Store that handles fetching files from object storage.
    cache: Arc<ChunkCache>,
}

impl Client {
    pub fn new(volume: Volume, max_cache_size: u64, chunk_size: u64, readahead: u64) -> Self {
        let cache = Arc::new(ChunkCache::new(
            max_cache_size,
            chunk_size,
            ReadAheadPolicy { size: readahead },
        ));

        Self { volume, cache }
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
        location: Location,
        byte_range: ByteRange,
    ) -> Result<&FileAttr> {
        self.volume
            .create(parent, name, exclusive, location, byte_range)
            .map_err(Error::from)
    }

    pub async fn open(&self, ino: Ino) -> Result<Pin<Box<dyn AsyncFileReader>>> {
        match ino {
            Ino::VERSION => {
                let version = self.volume.version().to_be_bytes();
                let reader = Box::pin(std::io::Cursor::new(version));
                return Ok(reader);
            }
            Ino::COMMIT => todo!(),
            ino => {
                let (location, byte_range) = self.volume.location(ino).ok_or(Error {
                    kind: ErrorKind::NotFound,
                })?;

                let file = File::new(self.cache.clone(), location.clone(), *byte_range).await;

                Ok(Box::pin(file))
            }
        }
    }

    pub fn delete(&mut self, parent: Ino, name: &str) -> Result<()> {
        self.volume.delete(parent, name)?;
        Ok(())
    }
}
