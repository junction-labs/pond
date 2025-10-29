use std::borrow::Cow;

pub type Result<T> = std::result::Result<T, crate::Error>;

#[non_exhaustive]
#[derive(Debug, Clone, Copy, thiserror::Error, PartialEq, Eq)]
pub enum ErrorKind {
    #[error("Is a directory")]
    IsADirectory,

    #[error("Not a directory")]
    NotADirectory,

    #[error("Directory not empty")]
    DirectoryNotEmpty,

    #[error("Already exists")]
    AlreadyExists,

    #[error("Not found")]
    NotFound,

    #[error("Permission denied")]
    PermissionDenied,

    #[error("Invalid data")]
    InvalidData,

    #[error("Timed out")]
    TimedOut,

    #[error("Unsupported operation")]
    Unsupported,

    #[error("Something went wrong")]
    Other,
}

#[derive(Debug, thiserror::Error)]
pub struct Error {
    kind: ErrorKind,

    context: Option<Cow<'static, str>>,

    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.context {
            Some(context) => write!(f, "{kind}: {context}", kind = self.kind),
            None => write!(f, "{}", self.kind),
        }
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self {
            kind,
            context: None,
            source: None,
        }
    }
}

impl Error {
    pub fn new<I>(kind: ErrorKind, context: I) -> Self
    where
        I: Into<Cow<'static, str>>,
    {
        let context = Some(context.into());
        Self {
            kind,
            context,
            source: None,
        }
    }

    pub fn with_source<E, I>(kind: ErrorKind, context: I, source: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
        I: Into<Cow<'static, str>>,
    {
        let context = Some(context.into());
        let source = Some(source.into());
        Self {
            kind,
            context,
            source,
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

impl From<&object_store::Error> for ErrorKind {
    fn from(e: &object_store::Error) -> Self {
        match e {
            object_store::Error::AlreadyExists { .. } => ErrorKind::AlreadyExists,
            object_store::Error::NotFound { .. } => ErrorKind::NotFound,
            object_store::Error::InvalidPath { .. } => ErrorKind::InvalidData,
            object_store::Error::PermissionDenied { .. }
            | object_store::Error::Unauthenticated { .. } => ErrorKind::PermissionDenied,
            _ => ErrorKind::Other,
        }
    }
}
