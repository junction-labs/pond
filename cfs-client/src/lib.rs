use cfs_core::{FileAttr, file::VolumeFile, read::ChunkedVolumeStore, volume::Volume};
use cfs_md::VolumeInfo;
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
    Volume(#[from] cfs_core::volume::VolumeError),

    #[error("open error: {0}")]
    Open(#[from] std::io::Error),

    #[error("unknown: {0}")]
    Unknown(Box<dyn std::error::Error + Send + Sync>),
}

/// A CoolFS client that provides access to a cluster and all of the volumes
/// it contains.
///
/// ```no_run
/// # use cfs_client::Client;
/// # use tokio::io::{AsyncRead, AsyncReadExt};
/// async fn process_data<R: AsyncRead>(r: R) { /* do stuff here */ }
///
/// # async fn doc() {
/// let client = Client::new();
///
/// assert_eq!(client.volumes().await.unwrap(), vec!["now_thats_what_i_call_data"]);
///
/// let mounted_client = client
///     .mount("now_thats_what_i_call_data", "vol3")
///     .await
///     .unwrap();
///
/// let reader = mounted_client.open(42, 0).await.unwrap();
/// process_data(reader);
/// # }
/// ```
#[derive(Clone)]
pub struct Client {
    /// HTTP Client for metadata
    http: reqwest::Client,
}

impl Client {
    pub fn new() -> Self {
        static USER_AGENT: &str = concat!("coolfs/", env!("CARGO_PKG_VERSION"));
        let http = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .build()
            .expect("failed to initialize tls backend");

        Self { http }
    }

    /// List all available volumes.
    ///
    /// *TODO*: This should return volume metadata.
    pub async fn volumes(&self) -> Result<Vec<String>> {
        let resp = self
            .http
            .get("http://localhost:8888/volumes")
            .send()
            .await?;

        let resp = resp.error_for_status()?;
        let bytes = resp.bytes().await?;

        let info = VolumeInfo::from_bytes(&bytes).map_err(|e| ErrorKind::Unknown(e.into()))?;
        Ok(info.names().map(|s| s.to_string()).collect())
    }

    /// List all versions of a specific volume.
    ///
    /// *TODO*: This should return volume-version metadata.
    pub async fn versions(&self, volume: &str) -> Result<Vec<String>> {
        let resp = self
            .http
            .get(format!("http://localhost:8888/volumes/{volume}"))
            .send()
            .await?;

        let resp = resp.error_for_status()?;
        let bytes = resp.bytes().await?;

        let info = VolumeInfo::from_bytes(&bytes).map_err(|e| ErrorKind::Unknown(e.into()))?;
        Ok(info.versions().map(|s| s.to_string()).collect())
    }

    /// Load a volume at a specific version. Returns an error if the
    /// volume doesn't exist or can't be loaded.
    pub async fn mount(
        &self,
        _volume: impl AsRef<str>,
        _version: impl AsRef<str>,
    ) -> Result<MountedClient> {
        unimplemented!()
    }
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

pub trait AsyncFileReader: AsyncRead + AsyncSeek {}
impl<T: AsyncRead + AsyncSeek> AsyncFileReader for T {}

pub struct MountedClient {
    /// HTTP Client for metadata
    http: reqwest::Client,

    // TODO: this could be multiple volume versions?
    // so if we have the most recent version, and it isn't compacted yet, then we kind of need a
    // chained-volume lookup here. this might be as simple as a ptr to another Volume, which we use
    // as a fallback if current version doesn't have anything.
    volume: Volume,

    /// Store that handles fetching files from object storage.
    object_store: Arc<ChunkedVolumeStore>,
}

impl MountedClient {
    pub fn getattr(&self, ino: u64) -> Result<&FileAttr> {
        match self.volume.stat(ino) {
            Some(attr) => Ok(attr),
            None => Err(Error {
                kind: ErrorKind::NotFound,
            }),
        }
    }

    pub fn lookup(&self, parent_ino: u64, name: String) -> Result<&FileAttr> {
        match self.volume.lookup(parent_ino, &name) {
            Some(attr) => Ok(attr),
            None => Err(Error {
                kind: ErrorKind::NotFound,
            }),
        }
    }

    pub async fn readdir(&self, ino: u64, _fh: u64, offset: u64) -> Result<Vec<(&str, &FileAttr)>> {
        let reader = self.volume.readdir(ino).map_err(Error::from)?;
        let entries: Vec<_> = reader.into_iter().skip(offset as usize).collect();
        Ok(entries)
    }

    pub async fn open(&self, ino: u64, _flags: i32) -> Result<Pin<Box<dyn AsyncFileReader>>> {
        let (location, byterange) = self.volume.location(ino).ok_or(Error {
            kind: ErrorKind::NotFound,
        })?;

        let file: Pin<Box<dyn AsyncFileReader>> = match location {
            cfs_core::Location::Local { path, .. } => {
                let file = tokio::fs::File::open(path).await.map_err(Error::from)?;
                Box::pin(file)
            }
            cfs_core::Location::ObjectStorage {
                bucket: _bucket,
                key,
                ..
            } => {
                let file =
                    VolumeFile::new(self.object_store.clone(), key.clone(), *byterange, None).await;
                Box::pin(file)
            }
            cfs_core::Location::Staged(i) => unimplemented!(),
        };

        Ok(file)
    }
}
