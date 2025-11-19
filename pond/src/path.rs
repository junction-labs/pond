use std::convert::TryInto;
use std::str::FromStr;

/// A canonical path to a file within Pond.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Path {
    pub(crate) raw: String,
}

impl Path {
    pub fn file_name(&self) -> &str {
        std::path::Path::new(&self.raw)
            .file_name()
            .and_then(|name| name.to_str())
            // all rust strings are valid OsStrs, but the opposite is not true. but we started with
            // a rust string, so we should be able to convert it back fine.
            .expect("BUG: raw string should be utf-8 valid and a canonical path")
    }

    pub fn path(&self) -> &str {
        &self.raw
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.raw
    }
}

impl std::fmt::Display for Path {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Returns true if `s` is a path that is in canonical form. Canonical paths are absolute (i.e.
/// start at the root) and do not contain any references (e.g. `.` or `..`).
fn is_canonical(s: &str) -> bool {
    if s.is_empty() || !s.starts_with('/') {
        return false;
    }

    std::path::Path::new(s).components().all(|c| {
        matches!(
            c,
            std::path::Component::RootDir | std::path::Component::Normal(..)
        )
    })
}

impl From<std::path::PathBuf> for Path {
    fn from(p: std::path::PathBuf) -> Self {
        Path {
            raw: p
                .into_os_string()
                .into_string()
                .expect("BUG: all paths should be valid utf-8"),
        }
    }
}

impl FromStr for Path {
    type Err = pond_core::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !is_canonical(s) {
            return Err(pond_core::Error::new(
                pond_core::ErrorKind::InvalidData,
                "path must be canonical",
            ));
        }
        Ok(Self { raw: s.to_string() })
    }
}

impl TryFrom<&str> for Path {
    type Error = pond_core::Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl TryFrom<String> for Path {
    type Error = pond_core::Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        if !is_canonical(&s) {
            return Err(pond_core::Error::new(
                pond_core::ErrorKind::InvalidData,
                "path must be canonical",
            ));
        }
        Ok(Self { raw: s })
    }
}

pub trait IntoPath {
    fn try_into_path(self) -> pond_core::Result<Path>;
}

impl IntoPath for Path {
    fn try_into_path(self) -> pond_core::Result<Path> {
        Ok(self)
    }
}

impl IntoPath for &Path {
    fn try_into_path(self) -> pond_core::Result<Path> {
        Ok(self.clone())
    }
}

impl<T> IntoPath for T
where
    T: TryInto<Path, Error = pond_core::Error>,
{
    fn try_into_path(self) -> pond_core::Result<Path> {
        self.try_into()
    }
}
