use object_store::{ObjectStore, aws::AmazonS3Builder, local::LocalFileSystem};
use std::{str::FromStr, sync::Arc};
use url::Url;

use crate::{Error, ErrorKind};

#[derive(Debug, Clone)]
pub struct RemoteStore {
    pub base_path: Arc<object_store::path::Path>,
    pub client: Arc<dyn ObjectStore>,
}

impl FromStr for RemoteStore {
    type Err = Error;

    /// Create an object_store::Client for a string.
    ///
    /// S3 scheme urls will use environment based credentials where possible and fall back to their
    /// default configurations.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match Url::parse(s) {
            Ok(url) => match url.clone().scheme() {
                // s3://<bucket>/<prefix>
                "s3" => {
                    let Some(bucket) = url.host_str() else {
                        return Err(Error::new(ErrorKind::InvalidData, "missing bucket"));
                    };

                    let s3 = AmazonS3Builder::from_env()
                        .with_bucket_name(bucket)
                        .build()
                        .map_err(|e| {
                            Error::new_context(
                                ErrorKind::Other,
                                "failed to build object store client",
                                e,
                            )
                        })?;
                    let base_path = object_store::path::Path::parse(url.path()).map_err(|e| {
                        Error::new_context(ErrorKind::InvalidData, "failed to parse url", e)
                    })?;
                    Ok(RemoteStore {
                        base_path: Arc::new(base_path),
                        client: Arc::new(s3),
                    })
                }
                // for a file:/// URL, make sure it presents as a URL with no
                // domain/authority so that people aren't expecting anything
                // odd.
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

                    let base_path = object_store::path::Path::parse(url.path()).map_err(|e| {
                        Error::new_context(ErrorKind::InvalidData, "failed to parse url", e)
                    })?;
                    Ok(RemoteStore {
                        base_path: Arc::new(base_path),
                        client: Arc::new(LocalFileSystem::new()),
                    })
                }
                scheme => Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("unsupported scheme: {scheme}"),
                )),
            },
            // parse path as a file path.
            Err(url::ParseError::RelativeUrlWithoutBase) => {
                let base_path = object_store::path::Path::from("/");
                Ok(RemoteStore {
                    base_path: Arc::new(base_path),
                    client: Arc::new(LocalFileSystem::new()),
                })
            }
            // not a valid location string
            Err(e) => Err(Error::new_context(
                ErrorKind::InvalidData,
                "failed to parse as url",
                e,
            )),
        }
    }
}

impl RemoteStore {
    pub async fn list_versions(&self) -> Result<Vec<u64>, object_store::Error> {
        let res = self
            .client
            .list_with_delimiter(Some(&self.base_path))
            .await?;

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

    pub async fn exists(
        &self,
        path: &object_store::path::Path,
    ) -> Result<bool, object_store::Error> {
        match self.client.head(path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    pub fn metadata(&self, version: u64) -> object_store::path::Path {
        self.base_path.child(format!("{version:016}.volume"))
    }

    pub fn new_data(&self) -> object_store::path::Path {
        let ident: u64 = rand::random();
        self.base_path.child(format!("{ident:016x}.data"))
    }
}
