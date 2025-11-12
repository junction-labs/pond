use std::{path::Path, str::FromStr, time::SystemTime};

use pond::{ErrorKind, FileAttr, Ino, Version};

/// Adapter for pond::Volume that allows it to operate on paths, rather than Ino and names.
pub(crate) struct VolumeAdapter {
    inner: pond::Volume,
}

impl VolumeAdapter {
    pub(crate) fn new(volume: pond::Volume) -> Self {
        Self { inner: volume }
    }
}

fn from_os_str(s: &std::ffi::OsStr) -> pond::Result<&str> {
    s.to_str().ok_or(pond::ErrorKind::InvalidData.into())
}

fn parent(path: &str) -> Option<&str> {
    let path = Path::new(path);
    path.parent()
        .map(|p| p.to_str().expect("should be convertible back to &str"))
}

/// Resolve a path to its FileAttr.
///
/// Walks the path components, resolving `FileAttr`s for each component along the way. Returns an
/// error if the path is invalid (e.g. does not exist, permissions, etc.).
async fn resolve_fileattr<'a>(
    volume: &'a pond::Volume,
    path: &'a str,
) -> pond::Result<&'a FileAttr> {
    let path = Path::new(path);

    let mut attr = volume.getattr(Ino::Root)?;
    for component in path.components() {
        match component {
            std::path::Component::RootDir => continue,
            std::path::Component::Normal(os_str) => {
                let name = from_os_str(os_str)?;
                attr = volume
                    .lookup(attr.ino, name)?
                    .ok_or(pond::ErrorKind::NotFound)?;
            }
            _ => {
                return Err(pond::Error::new(
                    pond::ErrorKind::Unsupported,
                    "Only absolute paths are supported",
                ));
            }
        }
    }

    Ok(attr)
}

/// Resolve a path to its direct parent Ino and its filename.
///
/// Returns an error if the path to the file does not exist. Does not check the file at the path
/// for existence.
async fn resolve_parent_ino_and_filename(
    volume: &pond::Volume,
    path: &str,
) -> pond::Result<(Ino, String)> {
    let parent = parent(path).ok_or(pond::ErrorKind::InvalidData)?;
    let parent_ino = resolve_fileattr(volume, parent).await?.ino;
    let name = Path::new(path)
        .file_name()
        .ok_or(pond::ErrorKind::InvalidData)?;
    let name = from_os_str(name)?;

    Ok((parent_ino, name.to_string()))
}

impl VolumeAdapter {
    pub(crate) async fn version(&self) -> Version {
        self.inner.version().clone()
    }

    /// Commit all staged changes in a volume, persisting it into the backend for this volume.
    pub(crate) async fn commit(&mut self, version: String) -> pond::Result<()> {
        let version = Version::from_str(&version)?;
        self.inner.commit(version).await
    }

    /// Fetch metadata of a file if it exists.
    pub(crate) async fn metadata(&self, path: String) -> pond::Result<FileAttr> {
        let attr = resolve_fileattr(&self.inner, &path).await?;
        Ok(attr.clone())
    }

    /// Check if a file or directory exists.
    pub(crate) async fn exists(&self, path: String) -> pond::Result<bool> {
        match resolve_fileattr(&self.inner, &path).await {
            Ok(_) => Ok(true),
            Err(e) => match e.kind() {
                ErrorKind::NotFound => Ok(false),
                _ => Err(e),
            },
        }
    }

    /// Iterate over the directory entries within the given directory.
    ///
    /// The return value holds a read-lock on the volume.
    pub(crate) async fn read_dir(&self, path: String) -> pond::Result<()> {
        let attr = resolve_fileattr(&self.inner, &path).await?;
        match attr.kind {
            pond::FileType::Regular => Err(pond::ErrorKind::NotADirectory.into()),
            pond::FileType::Directory => todo!(),
        }
    }

    /// Create a directory in the volume.
    pub(crate) async fn create_dir(&mut self, path: String) -> pond::Result<FileAttr> {
        let (parent_ino, name) = resolve_parent_ino_and_filename(&self.inner, &path).await?;
        self.inner.mkdir(parent_ino, name).cloned()
    }

    /// Create a directory in the volume.
    pub(crate) async fn create_dir_all(&mut self, _path: String) -> pond::Result<FileAttr> {
        todo!()
    }

    /// Remove a directory from the volume.
    pub(crate) async fn remove_dir(&mut self, path: String) -> pond::Result<()> {
        let (parent_ino, name) = resolve_parent_ino_and_filename(&self.inner, &path).await?;
        self.inner.rmdir(parent_ino, &name)
    }

    pub(crate) async fn remove_dir_all(&mut self, _path: String) -> pond::Result<()> {
        todo!()
    }

    /// Attempts to open a file in read-only mode.
    pub(crate) async fn open_read(&mut self, path: String) -> pond::Result<(pond::Fd, FileAttr)> {
        let attr = resolve_fileattr(&self.inner, &path).await?;
        let attr = attr.clone();
        let fd = self.inner.open_read(attr.ino).await?;
        Ok((fd, attr))
    }

    pub(crate) async fn open_read_write(
        &mut self,
        path: String,
    ) -> pond::Result<(pond::Fd, FileAttr)> {
        let attr = resolve_fileattr(&self.inner, &path).await?;
        let attr = attr.clone();
        let fd = self.inner.open_read_write(attr.ino).await?;
        Ok((fd, attr))
    }

    /// Release an open file.
    pub(crate) async fn release(&mut self, fd: pond::Fd) -> pond::Result<()> {
        self.inner.release(fd).await
    }

    pub(crate) async fn read_at(
        &mut self,
        fd: pond::Fd,
        offset: u64,
        size: usize,
    ) -> pond::Result<bytes::Bytes> {
        let mut buf = bytes::BytesMut::with_capacity(size);
        buf.resize(size, 0u8);
        let n = self.inner.read_at(fd, offset, &mut buf).await?;
        buf.truncate(n);
        Ok(buf.freeze())
    }

    pub(crate) async fn write_at(
        &mut self,
        fd: pond::Fd,
        offset: u64,
        buf: bytes::Bytes,
    ) -> pond::Result<usize> {
        self.inner.write_at(fd, offset, &buf).await
    }

    /// Remove a file from the volume.
    pub(crate) async fn remove_file(&mut self, path: String) -> pond::Result<()> {
        let (parent_ino, filename) = resolve_parent_ino_and_filename(&self.inner, &path).await?;
        self.inner.delete(parent_ino, &filename)
    }

    pub(crate) async fn copy(&mut self, _from: String, _to: String) -> pond::Result<()> {
        todo!(
            "do something smart for committed (identical Location) and do something dumb for staged (eager copy)"
        )
    }

    /// Move a file from src to dst.
    pub(crate) async fn rename(&mut self, src: String, dst: String) -> pond::Result<()> {
        let (src_parent, src_filename) = resolve_parent_ino_and_filename(&self.inner, &src).await?;
        let (dst_parent, dst_filename) = resolve_parent_ino_and_filename(&self.inner, &dst).await?;
        // this doesnt work, because rename dest shoudnt exist??
        self.inner
            .rename(src_parent, &src_filename, dst_parent, dst_filename)
    }

    /// Touch a file.
    ///
    /// Creates an empty file if it doesn't exist, updates the ctime otherwise.
    pub(crate) async fn touch(&mut self, path: String) -> pond::Result<FileAttr> {
        let (parent_ino, name) = resolve_parent_ino_and_filename(&self.inner, &path).await?;

        match self.inner.create(parent_ino, name.clone(), true) {
            Ok((attr, fd)) => {
                let attr = attr.clone();
                self.inner.release(fd).await?;
                Ok(attr)
            }
            Err(e) if e.kind() == pond::ErrorKind::AlreadyExists => {
                let ino = self
                    .inner
                    .lookup(parent_ino, &name)?
                    .ok_or(pond::ErrorKind::NotFound)?
                    .ino;
                let attr = self.inner.setattr(ino, None, Some(SystemTime::now()))?;
                Ok(attr.clone())
            }
            Err(e) => Err(e),
        }
    }
}
