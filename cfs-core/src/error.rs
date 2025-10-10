use std::borrow::Cow;

pub type Result<T> = std::result::Result<T, crate::Error>;

#[non_exhaustive]
#[derive(Debug, Clone, Copy, thiserror::Error, PartialEq, Eq)]
pub enum ErrorKind {
    #[error("is a directory")]
    IsADirectory,

    #[error("not a directory")]
    NotADirectory,

    #[error("directory not empty")]
    DirectoryNotEmpty,

    #[error("already exists")]
    AlreadyExists,

    #[error("not found")]
    NotFound,

    #[error("permission denied")]
    PermissionDenied,

    #[error("invalid data")]
    InvalidData,

    #[error("timed out")]
    TimedOut,

    #[error("unsupported operation")]
    Unsupported,

    #[error("something went wrong")]
    Other,
}

#[derive(Debug, thiserror::Error)]
pub struct Error {
    kind: ErrorKind,

    context: Option<Cow<'static, str>>,

    #[source]
    data: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.context {
            Some(context) => write!(f, "{context}: {kind}", kind = self.kind),
            None => write!(f, "{}", self.kind),
        }
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self {
            kind,
            context: None,
            data: None,
        }
    }
}

impl Error {
    pub fn new<E>(kind: ErrorKind, error: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let data = Some(error.into());
        Self {
            kind,
            context: None,
            data,
        }
    }

    pub fn new_context<E, I>(kind: ErrorKind, context: I, error: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
        I: Into<Cow<'static, str>>,
    {
        let data = Some(error.into());
        let context = Some(context.into());
        Self {
            kind,
            context,
            data,
        }
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl From<std::io::ErrorKind> for ErrorKind {
    fn from(io_kind: std::io::ErrorKind) -> Self {
        match io_kind {
            std::io::ErrorKind::NotFound => ErrorKind::NotFound,
            std::io::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
            std::io::ErrorKind::AlreadyExists => ErrorKind::AlreadyExists,
            std::io::ErrorKind::NotADirectory => ErrorKind::NotADirectory,
            std::io::ErrorKind::IsADirectory => ErrorKind::IsADirectory,
            std::io::ErrorKind::DirectoryNotEmpty => ErrorKind::DirectoryNotEmpty,
            std::io::ErrorKind::InvalidInput => ErrorKind::InvalidData,
            std::io::ErrorKind::TimedOut => ErrorKind::TimedOut,
            std::io::ErrorKind::Unsupported => ErrorKind::Unsupported,
            _ => ErrorKind::Other,
        }
    }
}
