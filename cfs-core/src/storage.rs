use futures::TryFutureExt;
use object_store::{ObjectStore, aws::AmazonS3Builder, local::LocalFileSystem};
use std::sync::Arc;
use tempfile::TempDir;
use url::Url;

use crate::{Error, ErrorKind, Result, VolumeMetadata};

#[derive(Debug, Clone)]
pub(crate) struct Storage {
    pub(crate) temp_dir: Arc<TempDir>,
    pub(crate) base_path: Option<object_store::path::Path>,
    pub(crate) remote: Arc<dyn ObjectStore>,
}

impl Storage {
    /// Create an object_store::Client for a string.
    ///
    /// S3 scheme urls will use environment based credentials where possible and fall back to their
    /// default configurations.
    pub(crate) fn for_volume(s: &str) -> Result<Self> {
        match Url::parse(s) {
            Ok(url) => match url.scheme() {
                "memory" => Ok(Self::new_in_memory()),
                "s3" => Self::new_s3(&url),
                "file" => {
                    if !url.authority().is_empty() {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            format!(
                                "file-scheme urls should have an empty authority. found: {}",
                                url.authority()
                            ),
                        ));
                    }
                    Self::new_filesystem(url.path())
                }
                scheme => Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("unsupported scheme: {scheme}"),
                )),
            },
            // hope the whole thing is path.
            Err(url::ParseError::RelativeUrlWithoutBase) => Self::new_filesystem(s),
            // not a valid location string
            Err(e) => Err(Error::new_context(
                ErrorKind::InvalidData,
                "failed to parse as url",
                e,
            )),
        }
    }

    pub(crate) fn new_in_memory() -> Self {
        let client = object_store::memory::InMemory::new();
        let temp_dir = tempfile::Builder::new().prefix(".cfs").tempdir().unwrap();

        Storage {
            base_path: None,
            temp_dir: Arc::new(temp_dir),
            remote: Arc::new(client),
        }
    }

    pub(crate) fn new_s3(url: &Url) -> Result<Self> {
        let Some(bucket) = url.host_str() else {
            return Err(Error::new(ErrorKind::InvalidData, "missing bucket"));
        };
        let base_path = object_store::path::Path::parse(url.path())
            .map_err(|e| Error::new_context(ErrorKind::InvalidData, "failed to parse url", e))?;

        let temp_dir = tempfile::Builder::new()
            .prefix(".cfs")
            .tempdir()
            .map_err(|e| Error::new_context(e.kind().into(), "failed to create tempdir", e))?;

        let s3 = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()
            .map_err(|e| {
                Error::new_context(ErrorKind::Other, "failed to build object store client", e)
            })?;

        Ok(Storage {
            base_path: Some(base_path),
            temp_dir: Arc::new(temp_dir),
            remote: Arc::new(s3),
        })
    }

    pub(crate) fn new_filesystem(base_path: impl AsRef<std::path::Path>) -> Result<Self> {
        // use the URL path as the base path for the tempdir and the client, but
        // don't actually save it as base_path - we want paths relative
        // to the local filesystem client
        let base_path = base_path.as_ref();
        let temp_dir = tempfile::Builder::new()
            .prefix(".cfs")
            .tempdir_in(base_path)
            .map_err(|e| Error::new_context(e.kind().into(), "failed to create tempdir", e))?;

        let client = LocalFileSystem::new_with_prefix(base_path).unwrap();

        Ok(Storage {
            base_path: None,
            temp_dir: Arc::new(temp_dir),
            remote: Arc::new(client),
        })
    }
}

impl Storage {
    pub(crate) fn tempfile(&self) -> Result<(std::path::PathBuf, tokio::fs::File)> {
        let f = tempfile::Builder::new()
            .disable_cleanup(true)
            .tempfile_in(&*self.temp_dir)
            .map_err(|e| Error::new_context(e.kind().into(), "failed to create tempfile", e))?;

        let (f, path) = f.into_parts();
        Ok((path.to_path_buf(), tokio::fs::File::from_std(f)))
    }

    pub(crate) async fn list_versions(&self) -> Result<Vec<u64>> {
        let res = self
            .remote
            .list_with_delimiter(self.base_path.as_ref())
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { .. } => {
                    Error::new_context(ErrorKind::NotFound, "no such volume", e)
                }
                object_store::Error::InvalidPath { .. } => {
                    Error::new_context(ErrorKind::InvalidData, "invalid volume path", e)
                }
                _ => Error::new_context(ErrorKind::Other, "failed to list versions", e),
            })?;

        let mut versions = vec![];
        for object in res.objects {
            let Some(filename) = object.location.filename() else {
                continue;
            };

            if let Some(version_str) = filename.strip_suffix(".volume")
                && let Ok(v) = version_str.parse()
            {
                versions.push(v);
            }
        }
        Ok(versions)
    }

    pub(crate) async fn latest_version(&self) -> Result<u64> {
        let versions = self.list_versions().await?;
        let version = versions
            .iter()
            .max()
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "not a cfs volume"))?;
        Ok(*version)
    }

    pub(crate) async fn load_version(&self, version: u64) -> Result<VolumeMetadata> {
        let path = self.metadata(version);
        let get = self.remote.get(&path).and_then(|res| res.bytes());
        let bytes = get.await.map_err(|e| {
            let kind = match e {
                object_store::Error::NotFound { .. } => ErrorKind::NotFound,
                object_store::Error::InvalidPath { .. } => ErrorKind::InvalidData,
                object_store::Error::PermissionDenied { .. }
                | object_store::Error::Unauthenticated { .. } => ErrorKind::PermissionDenied,
                _ => ErrorKind::Other,
            };
            Error::new_context(kind, "error loading volume metdata", e)
        })?;
        VolumeMetadata::from_bytes(&bytes)
    }

    pub(crate) async fn exists(&self, path: &object_store::path::Path) -> Result<bool> {
        match self.remote.head(path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => {
                let error_kind = match e {
                    object_store::Error::InvalidPath { .. } => ErrorKind::InvalidData,
                    object_store::Error::PermissionDenied { .. }
                    | object_store::Error::Unauthenticated { .. } => ErrorKind::PermissionDenied,
                    _ => ErrorKind::Other,
                };
                Err(Error::new_context(error_kind, "failed to head path", e))
            }
        }
    }

    pub(crate) fn metadata(&self, version: u64) -> object_store::path::Path {
        let part = format!("{version:016}.volume");
        match &self.base_path {
            Some(p) => p.child(part),
            None => object_store::path::Path::from(part),
        }
    }

    pub(crate) fn new_data(&self) -> object_store::path::Path {
        let ident: u64 = rand::random();
        let part = format!("{ident:016x}.data");
        match &self.base_path {
            Some(p) => p.child(part),
            None => object_store::path::Path::from(part),
        }
    }
}
