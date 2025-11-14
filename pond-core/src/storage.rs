use futures::TryFutureExt;
use object_store::{ObjectStore, aws::AmazonS3Builder, local::LocalFileSystem};
use std::sync::Arc;
use tempfile::TempDir;
use url::Url;

use crate::{
    Error, ErrorKind, Result,
    metadata::{Version, VolumeMetadata},
};

#[derive(Debug, Clone)]
pub(crate) struct Storage {
    pub(crate) temp_dir: Arc<TempDir>,
    pub(crate) base_path: Option<object_store::path::Path>,
    pub(crate) remote: Arc<dyn ObjectStore>,
}

impl Storage {
    /// Create an Storage instance from a location string.
    ///
    /// S3 scheme urls will use environment based credentials where possible and fall back to their
    /// default configurations.
    pub(crate) fn for_location(s: &str, create: bool) -> Result<Self> {
        match Url::parse(s) {
            Ok(url) => match url.scheme() {
                "memory" => Ok(Self::new_in_memory()?),
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
                    Self::new_filesystem(url.path(), create)
                }
                scheme => Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("unsupported scheme: {scheme}"),
                )),
            },
            // hope the whole thing is path.
            Err(url::ParseError::RelativeUrlWithoutBase) => Self::new_filesystem(s, create),
            // not a valid location string
            Err(e) => Err(Error::with_source(
                ErrorKind::InvalidData,
                format!("failed to parse {s} as url"),
                e,
            )),
        }
    }

    pub(crate) fn object_store_description(&self) -> String {
        match &self.base_path {
            Some(path) => {
                format!("{}://{}", self.remote, path.as_ref())
            }
            None => {
                format!("{}", self.remote)
            }
        }
    }

    pub(crate) fn staged_file_temp_dir(&self) -> Arc<TempDir> {
        self.temp_dir.clone()
    }

    pub(crate) fn new_in_memory() -> Result<Self> {
        let client = object_store::memory::InMemory::new();
        let temp_dir = tempfile::Builder::new()
            .prefix(".pond")
            .tempdir()
            .map_err(|e| Error::with_source(e.kind().into(), "failed to create tempdir", e))?;

        Ok(Storage {
            base_path: None,
            temp_dir: Arc::new(temp_dir),
            remote: Arc::new(client),
        })
    }

    pub(crate) fn new_s3(url: &Url) -> Result<Self> {
        let Some(bucket) = url.host_str() else {
            return Err(Error::new(ErrorKind::InvalidData, "missing s3 bucket"));
        };
        let base_path = object_store::path::Path::parse(url.path()).map_err(|e| {
            Error::with_source(
                ErrorKind::InvalidData,
                format!("failed to parse {url} as a object_store path"),
                e,
            )
        })?;

        let temp_dir = tempfile::Builder::new()
            .prefix(".pond")
            .tempdir()
            .map_err(|e| Error::with_source(e.kind().into(), "failed to create tempdir", e))?;

        let s3 = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()
            .map_err(|e| {
                Error::with_source(
                    ErrorKind::Other,
                    "failed to build s3 object store client",
                    e,
                )
            })?;

        Ok(Storage {
            base_path: Some(base_path),
            temp_dir: Arc::new(temp_dir),
            remote: Arc::new(s3),
        })
    }

    pub(crate) fn new_filesystem(
        base_path: impl AsRef<std::path::Path>,
        create: bool,
    ) -> Result<Self> {
        if !base_path.as_ref().exists() && create {
            std::fs::create_dir(&base_path).map_err(|e| {
                let kind = e.kind().into();
                Error::with_source(
                    kind,
                    format!(
                        "unable to create volume at {}",
                        base_path.as_ref().to_string_lossy()
                    ),
                    e,
                )
            })?;
        }

        // use the URL path as the base path for the tempdir and the client, but
        // don't actually save it as base_path - we want paths relative
        // to the local filesystem client
        let base_path = match std::fs::canonicalize(&base_path) {
            Ok(path) => path,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::with_source(
                    ErrorKind::NotFound,
                    format!("{} does not exist", base_path.as_ref().to_string_lossy()),
                    e,
                ));
            }
            Err(e) => {
                return Err(Error::with_source(
                    e.kind().into(),
                    "failed to set up base path",
                    e,
                ));
            }
        };

        let temp_dir = tempfile::Builder::new()
            .prefix(".pond")
            .tempdir_in(&base_path)
            .map_err(|e| Error::with_source(e.kind().into(), "failed to create tempdir", e))?;

        let client = LocalFileSystem::new();
        let base_path = match base_path.to_str() {
            Some(p) => object_store::path::Path::from(p),
            None => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "base path must be valid utf-8",
                ));
            }
        };

        Ok(Storage {
            base_path: Some(base_path),
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
            .map_err(|e| Error::with_source(e.kind().into(), "failed to create tempfile", e))?;

        let (f, path) = f.into_parts();
        Ok((path.to_path_buf(), tokio::fs::File::from_std(f)))
    }

    pub(crate) async fn list_versions(&self) -> Result<Vec<Version>> {
        let res = self
            .remote
            .list_with_delimiter(self.base_path.as_ref())
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { path, source } => Error::with_source(
                    ErrorKind::NotFound,
                    format!("no such volume at {path}"),
                    source,
                ),
                object_store::Error::InvalidPath { source } => {
                    Error::with_source(ErrorKind::InvalidData, "invalid volume path", source)
                }
                _ => Error::with_source(ErrorKind::Other, "failed to list versions", e),
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

    pub(crate) async fn latest_version(&self) -> Result<Version> {
        let versions = self.list_versions().await?;
        let version = versions
            .into_iter()
            .max()
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "no versions found"))?;
        Ok(version)
    }

    pub(crate) async fn load_version(&self, version: &Version) -> Result<VolumeMetadata> {
        let path = self.metadata_path(version);
        let get = self.remote.get(&path).and_then(|res| res.bytes());
        let bytes = get.await.map_err(|e| {
            Error::with_source(
                (&e).into(),
                format!("failed to read volume metadata for version={version}"),
                e,
            )
        })?;
        VolumeMetadata::from_bytes(&bytes)
    }

    pub(crate) async fn exists(&self, version: &Version) -> Result<bool> {
        let path = &self.metadata_path(version);
        match self.remote.head(path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(Error::with_source(
                (&e).into(),
                format!("failed to head {path} for version={version}"),
                e,
            )),
        }
    }

    pub(crate) fn child_path(&self, name: &str) -> object_store::path::Path {
        match &self.base_path {
            // FIXME: lol, lmao, etc.
            Some(base) => base.child(name),
            None => object_store::path::Path::from(name),
        }
    }

    pub(crate) fn metadata_path(&self, version: &Version) -> object_store::path::Path {
        let part = format!("{version}.volume");
        self.child_path(&part)
    }

    pub(crate) fn new_data_file(&self) -> (String, object_store::path::Path) {
        let ident: u64 = rand::random();
        let filename = format!("{ident:016x}.data");
        let child = self.child_path(&filename);
        (filename, child)
    }
}
