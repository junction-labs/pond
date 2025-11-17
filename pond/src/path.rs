use std::{fmt, str::FromStr};

/// A canonical path to a file within Pond.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Path {
    pub(crate) raw: String,
}

impl Path {
    pub(crate) fn as_str(&self) -> &str {
        &self.raw
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Returns true if `s` is a path that is in canonical form. Canonical paths are absolute (i.e.
/// start at the root) and don't contain any references (e.g. `.` or `..`).
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
