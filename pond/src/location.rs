use std::{borrow::Cow, sync::Arc};

// this is a newtype around a usize, but we don't want to let it be used as
// anything other than a reference so that holding one keeps a borrow on a
// Volume. don't derive Clone or Copy.
#[derive(Debug)]
#[repr(transparent)]
pub struct LocationId(pub(crate) usize);

impl AsRef<LocationId> for usize {
    fn as_ref(&self) -> &LocationId {
        unsafe { std::mem::transmute(self) }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Location {
    Staged { path: std::path::PathBuf },
    Committed { key: Arc<str> },
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

    pub(crate) fn committed<'a>(key: impl Into<Cow<'a, str>>) -> Self {
        let key = match key.into() {
            Cow::Borrowed(str) => Arc::from(str),
            Cow::Owned(string) => Arc::from(string.into_boxed_str()),
        };
        Location::Committed { key }
    }

    pub(crate) fn staged(path: impl AsRef<std::path::Path>) -> Self {
        Location::Staged {
            path: path.as_ref().to_path_buf(),
        }
    }
}
