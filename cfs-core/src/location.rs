use std::{borrow::Cow, str::FromStr};
use url::Url;

#[derive(Debug, thiserror::Error)]
pub enum InvalidLocation {
    #[error("unsupported url scheme: {0}")]
    UnsupportedScheme(String),

    #[error("files with an authority are not supported")]
    FileHasAuthority,

    #[error("url doesn't specify a bucket")]
    MissingBucket,

    #[error(transparent)]
    InvalidPath(#[from] object_store::path::Error),

    #[error("invalid url")]
    InvalidUrl,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Location {
    Staged {
        path: std::path::PathBuf,
    },
    Local {
        path: std::path::PathBuf,
    },
    ObjectStorage {
        bucket: String,
        key: object_store::path::Path,
    },
}

impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Location::Staged { path } | Location::Local { path } => write!(f, "{}", path.display()),
            Location::ObjectStorage { bucket, key } => write!(f, "s3://{bucket}/{key}"),
        }
    }
}

impl FromStr for Location {
    type Err = InvalidLocation;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match Url::parse(s) {
            Ok(url) => match url.scheme() {
                // s3://<bucket>/<prefix>
                "s3" => {
                    let Some(bucket) = url.host_str() else {
                        return Err(InvalidLocation::MissingBucket);
                    };
                    // TODO: validate bucket names.
                    let bucket = bucket.to_string();
                    let key = object_store::path::Path::parse(url.path())?;
                    Ok(Location::ObjectStorage { bucket, key })
                }
                // for a file:/// URL, make sure it presents as a URL with no
                // domain/authority so that people aren't expecting anything
                // odd.
                "file" => {
                    if url.has_authority() {
                        return Err(InvalidLocation::FileHasAuthority);
                    }
                    let path = std::path::PathBuf::from(url.path());
                    Ok(Location::Local { path })
                }
                scheme => Err(InvalidLocation::UnsupportedScheme(scheme.to_string())),
            },
            // parse path as a file path.
            Err(url::ParseError::RelativeUrlWithoutBase) => {
                let path = std::path::PathBuf::from(s);
                Ok(Location::Local { path })
            }
            // not a valid location string
            Err(_) => Err(InvalidLocation::InvalidUrl),
        }
    }
}

impl Location {
    pub fn is_staged(&self) -> bool {
        matches!(self, Location::Staged { .. })
    }

    pub fn object_storage(bucket: impl AsRef<str>, key: impl AsRef<str>) -> Self {
        Location::ObjectStorage {
            bucket: bucket.as_ref().to_string(),
            key: object_store::path::Path::from(key.as_ref()),
        }
    }

    pub fn local(path: impl AsRef<std::path::Path>) -> Self {
        Location::Local {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn staged(path: impl AsRef<std::path::Path>) -> Self {
        Location::Staged {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn metadata(&self, version: u64) -> Location {
        self.child(&format!("{version:016}.volume"))
    }

    pub fn new_data(&self) -> Location {
        let ident: u64 = rand::random();
        self.child(&format!("{ident:016x}.data"))
    }

    fn child(&self, part: &str) -> Self {
        match self {
            Location::Staged { path } => Location::Staged {
                path: path.join(part),
            },
            Location::Local { path } => Location::Local {
                path: path.join(part),
            },
            Location::ObjectStorage { bucket, key } => {
                let key = key.child(part);
                Location::ObjectStorage {
                    bucket: bucket.clone(),
                    key,
                }
            }
        }
    }

    pub fn local_path(&self) -> Option<&std::path::Path> {
        match self {
            Location::Staged { path } | Location::Local { path } => Some(path),
            Location::ObjectStorage { .. } => None,
        }
    }

    pub fn path(&self) -> Cow<'_, object_store::path::Path> {
        match self {
            Location::Staged { path } | Location::Local { path } => Cow::Owned(
                object_store::path::Path::from(path.to_string_lossy().as_ref()),
            ),
            Location::ObjectStorage { key, .. } => Cow::Borrowed(key),
        }
    }
}
