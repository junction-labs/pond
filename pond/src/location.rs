use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Location {
    Staged { path: std::path::PathBuf },
    Committed { key: Arc<object_store::path::Path> },
}

impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Location::Staged { path } => write!(f, "{}", path.display()),
            Location::Committed { key } => write!(f, "{key}"),
        }
    }
}

impl Location {
    pub(crate) fn is_staged(&self) -> bool {
        matches!(self, Location::Staged { .. })
    }

    pub(crate) fn committed(key: impl AsRef<str>) -> Self {
        Location::Committed {
            key: Arc::new(object_store::path::Path::from(key.as_ref())),
        }
    }

    pub(crate) fn staged(path: impl AsRef<std::path::Path>) -> Self {
        Location::Staged {
            path: path.as_ref().to_path_buf(),
        }
    }
}
