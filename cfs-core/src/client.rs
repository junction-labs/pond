use crate::{
    FileAttr, Ino,
    file::File,
    read::{ChunkCache, ReadAheadPolicy},
    volume::Volume,
};
use std::{pin::Pin, sync::Arc};
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
    pub fn new(volume: Volume, chunk_size: u64, readahead: u64) -> Self {
        let cache = Arc::new(ChunkCache::new(
            chunk_size,
            ReadAheadPolicy { size: readahead },
        ));

        Self { volume, cache }
    }

    pub fn getattr(&self, ino: Ino) -> Result<&FileAttr> {
        match self.volume.stat(ino) {
            Some(attr) => Ok(attr),
            None => Err(Error {
                kind: ErrorKind::NotFound,
            }),
        }
    }

    pub fn lookup(&self, parent: Ino, name: &str) -> Result<Option<&FileAttr>> {
        self.volume.lookup(parent, name).map_err(|e| e.into())
    }

    pub fn readdir(&self, ino: Ino) -> Result<impl Iterator<Item = (&str, &FileAttr)>> {
        self.volume.readdir(ino).map_err(Error::from)
    }

    pub async fn open(&self, ino: Ino) -> Result<Pin<Box<dyn AsyncFileReader>>> {
        if let Ino::VERSION = ino {
            let reader = Box::pin(std::io::Cursor::new(self.volume.version_data()));
            return Ok(reader);
        }

        let (location, byte_range) = self.volume.location(ino).ok_or(Error {
            kind: ErrorKind::NotFound,
        })?;

        let file = File::new(self.cache.clone(), location.clone(), *byte_range).await;

        Ok(Box::pin(file))
    }
}
