use std::{
    path::{Component, Path, PathBuf},
    str::FromStr,
    time::SystemTime,
};

use pond_core::{ErrorKind, FileAttr, FileType, Ino, Version};

/// Adapter for pond_core::Volume that allows it to operate on paths, rather than Ino and names.
pub(crate) struct VolumeAdapter {
    inner: pond_core::Volume,
}

impl VolumeAdapter {
    pub(crate) fn new(volume: pond_core::Volume) -> Self {
        Self { inner: volume }
    }
}

#[derive(Clone, Debug)]
pub struct DirEntry {
    path: PathBuf,
    file_name: String,
    attr: FileAttr,
}

impl DirEntry {
    pub fn new(path: PathBuf, file_name: String, attr: FileAttr) -> Self {
        Self {
            path,
            file_name,
            attr,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    pub fn file_type(&self) -> FileType {
        self.attr.kind
    }

    pub fn attr(&self) -> &FileAttr {
        &self.attr
    }
}

fn from_os_str(s: &std::ffi::OsStr) -> pond_core::Result<&str> {
    s.to_str().ok_or(pond_core::ErrorKind::InvalidData.into())
}

fn parent(path: &str) -> Option<&str> {
    let path = Path::new(path);
    path.parent()
        .map(|p| p.to_str().expect("should be convertible back to &str"))
}

/// Resolve a path to its FileAttr.
///
/// Walks the path components, resolving `FileAttr`s for each component along the way. Returns an
/// error if the path is invalid (e.g. does not exist, permissions, not an aboslute path, etc.).
async fn resolve_fileattr<'a>(
    volume: &'a pond_core::Volume,
    path: &'a str,
) -> pond_core::Result<&'a FileAttr> {
    let path = Path::new(path);

    let mut attr = volume.getattr(Ino::Root)?;
    for component in path.components() {
        match component {
            Component::RootDir => continue,
            Component::Normal(os_str) => {
                let name = from_os_str(os_str)?;
                attr = volume
                    .lookup(attr.ino, name)?
                    .ok_or(pond_core::ErrorKind::NotFound)?;
            }
            _ => {
                return Err(pond_core::Error::new(
                    pond_core::ErrorKind::Unsupported,
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
    volume: &pond_core::Volume,
    path: &str,
) -> pond_core::Result<(Ino, String)> {
    let parent = parent(path).ok_or(pond_core::ErrorKind::InvalidData)?;
    let parent_ino = resolve_fileattr(volume, parent).await?.ino;
    let name = Path::new(path)
        .file_name()
        .ok_or(pond_core::ErrorKind::InvalidData)?;
    let name = from_os_str(name)?;

    Ok((parent_ino, name.to_string()))
}

impl VolumeAdapter {
    pub(crate) async fn version(&self) -> Version {
        self.inner.version().clone()
    }

    /// Commit all staged changes in a volume, persisting it into the backend for this volume.
    pub(crate) async fn commit(&mut self, version: String) -> pond_core::Result<()> {
        let version = Version::from_str(&version)?;
        self.inner.commit(version).await
    }

    /// Fetch metadata of a file if it exists.
    pub(crate) async fn metadata(&self, path: String) -> pond_core::Result<FileAttr> {
        let attr = resolve_fileattr(&self.inner, &path).await?;
        Ok(attr.clone())
    }

    /// Check if a file or directory exists.
    pub(crate) async fn exists(&self, path: String) -> pond_core::Result<bool> {
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
    /// Takes an offset and length to do paginated reads, since the contents of the directory may be
    /// large. Pass the last filename of the last file you received from this function as the
    /// offset to resume where the last call left off.
    pub(crate) async fn read_dir(
        &self,
        path: String,
        offset: Option<String>,
        len: usize,
    ) -> pond_core::Result<Vec<crate::DirEntry>> {
        let attr = resolve_fileattr(&self.inner, &path).await?;
        if attr.kind != pond_core::FileType::Directory {
            return Err(pond_core::ErrorKind::NotADirectory.into());
        }

        let parent = Path::new(&path);
        let entries = self
            .inner
            .readdir(attr.ino)?
            .filter(|entry| entry.attr().ino.is_regular())
            .skip_while(|entry| match &offset {
                Some(offset) => *entry.name() <= **offset,
                None => false,
            })
            .take(len)
            .map(|entry| crate::DirEntry {
                path: parent.join(entry.name()),
                file_name: entry.name().to_string(),
                attr: entry.attr().clone(),
            })
            .collect();

        Ok(entries)
    }

    /// Create a directory in the volume.
    pub(crate) async fn create_dir(&mut self, path: String) -> pond_core::Result<FileAttr> {
        let (parent_ino, name) = resolve_parent_ino_and_filename(&self.inner, &path).await?;
        self.inner.mkdir(parent_ino, name).cloned()
    }

    pub(crate) async fn create_dir_all(&mut self, path: String) -> pond_core::Result<FileAttr> {
        let path = Path::new(&path);

        let mut ino = self.inner.getattr(pond_core::Ino::Root)?.ino;
        let mut components = path.components().peekable();
        while let Some(component) = components.next() {
            match component {
                Component::RootDir => continue,
                Component::Normal(os_str) => {
                    let name = from_os_str(os_str)?;
                    match self.inner.lookup(ino, name)? {
                        Some(f) => match f.kind {
                            pond_core::FileType::Regular => {
                                let kind = match components.peek() {
                                    // if this path component is the last component, we return
                                    // AlreadyExists even if it's the wrong FileType.
                                    Some(_) => pond_core::ErrorKind::NotADirectory,
                                    // otherwise, we say that it's not a directory (and this path is
                                    // actually invalid because the middle of it contains a file).
                                    None => pond_core::ErrorKind::AlreadyExists,
                                };
                                return Err(kind.into());
                            }
                            pond_core::FileType::Directory => ino = f.ino,
                        },
                        None => {
                            let created = self.inner.mkdir(ino, name.to_string())?;
                            ino = created.ino;
                        }
                    }
                }
                _ => {
                    return Err(pond_core::Error::new(
                        pond_core::ErrorKind::Unsupported,
                        "Only absolute paths are supported",
                    ));
                }
            }
        }

        Ok(self.inner.getattr(ino)?.clone())
    }

    /// Remove a directory from the volume.
    pub(crate) async fn remove_dir(&mut self, path: String) -> pond_core::Result<()> {
        let (parent_ino, name) = resolve_parent_ino_and_filename(&self.inner, &path).await?;
        self.inner.rmdir(parent_ino, &name)
    }

    pub(crate) async fn remove_dir_all(&mut self, path: String) -> pond_core::Result<()> {
        let attr = resolve_fileattr(&self.inner, &path).await?;
        if attr.kind != pond_core::FileType::Directory {
            return Err(pond_core::ErrorKind::NotADirectory.into());
        }

        let (parent_ino, name) = resolve_parent_ino_and_filename(&self.inner, &path).await?;
        self.remove_dir_contents(attr.ino)?;
        self.inner.rmdir(parent_ino, &name)
    }

    fn remove_dir_contents(&mut self, ino: Ino) -> pond_core::Result<()> {
        let entries: Vec<_> = self
            .inner
            .readdir(ino)?
            .map(|e| (e.name().to_string(), e.attr().clone()))
            .collect();

        for (name, attr) in entries {
            match attr.kind {
                pond_core::FileType::Directory => {
                    self.remove_dir_contents(attr.ino)?;
                    self.inner.rmdir(ino, &name)?;
                }
                pond_core::FileType::Regular => {
                    self.inner.delete(ino, &name)?;
                }
            }
        }

        Ok(())
    }

    /// Attempts to open a file in read-only mode.
    pub(crate) async fn open_read(
        &mut self,
        path: String,
    ) -> pond_core::Result<(pond_core::Fd, FileAttr)> {
        let attr = resolve_fileattr(&self.inner, &path).await?;
        let attr = attr.clone();
        let fd = self.inner.open_read(attr.ino).await?;
        Ok((fd, attr))
    }

    pub(crate) async fn open_read_write(
        &mut self,
        path: String,
    ) -> pond_core::Result<(pond_core::Fd, FileAttr)> {
        let attr = resolve_fileattr(&self.inner, &path).await?;
        let attr = attr.clone();
        let fd = self.inner.open_read_write(attr.ino).await?;
        Ok((fd, attr))
    }

    /// Release an open file.
    pub(crate) async fn release(&mut self, fd: pond_core::Fd) -> pond_core::Result<()> {
        self.inner.release(fd).await
    }

    pub(crate) async fn read_at(
        &mut self,
        fd: pond_core::Fd,
        offset: u64,
        size: usize,
    ) -> pond_core::Result<bytes::Bytes> {
        let mut buf = bytes::BytesMut::with_capacity(size);
        buf.resize(size, 0u8);
        let n = self.inner.read_at(fd, offset, &mut buf).await?;
        buf.truncate(n);
        Ok(buf.freeze())
    }

    pub(crate) async fn write_at(
        &mut self,
        fd: pond_core::Fd,
        offset: u64,
        buf: bytes::Bytes,
    ) -> pond_core::Result<usize> {
        self.inner.write_at(fd, offset, &buf).await
    }

    /// Remove a file from the volume.
    pub(crate) async fn remove_file(&mut self, path: String) -> pond_core::Result<()> {
        let (parent_ino, filename) = resolve_parent_ino_and_filename(&self.inner, &path).await?;
        self.inner.delete(parent_ino, &filename)
    }

    pub(crate) async fn copy(&mut self, _from: String, _to: String) -> pond_core::Result<()> {
        unimplemented!(
            "do something smart for committed (identical Location)? and do something dumb for staged (eager copy)?"
        )
    }

    /// Move a file from src to dst.
    pub(crate) async fn rename(&mut self, src: String, dst: String) -> pond_core::Result<()> {
        let (src_parent, src_filename) = resolve_parent_ino_and_filename(&self.inner, &src).await?;
        let (dst_parent, dst_filename) = resolve_parent_ino_and_filename(&self.inner, &dst).await?;
        // this doesnt work, because rename dest shoudnt exist??
        self.inner
            .rename(src_parent, &src_filename, dst_parent, dst_filename)
    }

    /// Touch a file.
    ///
    /// Creates an empty file if it doesn't exist, updates the ctime otherwise.
    pub(crate) async fn touch(&mut self, path: String) -> pond_core::Result<FileAttr> {
        let (parent_ino, name) = resolve_parent_ino_and_filename(&self.inner, &path).await?;

        match self.inner.create(parent_ino, name.clone(), true) {
            Ok((attr, fd)) => {
                let attr = attr.clone();
                self.inner.release(fd).await?;
                Ok(attr)
            }
            Err(e) if e.kind() == pond_core::ErrorKind::AlreadyExists => {
                let ino = self
                    .inner
                    .lookup(parent_ino, &name)?
                    .ok_or(pond_core::ErrorKind::NotFound)?
                    .ino;
                let attr = self.inner.setattr(ino, None, Some(SystemTime::now()))?;
                Ok(attr.clone())
            }
            Err(e) => Err(e),
        }
    }
}
