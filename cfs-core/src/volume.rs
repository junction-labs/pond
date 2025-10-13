use crate::{
    ByteRange, Error, FileAttr, Ino, Location, Result,
    cache::{ChunkCache, ReadAheadPolicy},
    error::ErrorKind,
    metadata::{Modify, VolumeMetadata},
};
use bytes::Bytes;
use object_store::{ObjectStore, PutOptions, PutPayload};
use std::{collections::BTreeMap, io::SeekFrom, path::PathBuf, sync::Arc, time::SystemTime};
use tempfile::TempDir;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

// TODO: We should use our own Path type abstraction here. std::fs::Path
// is pretty close to right but offers a bunch of system methods (like canonicalize)
// that mean it will do unexpected things if we use it as part of our interface.
//
// Both AWS and GCS commit to key names being valid utf8 but random filesystems we
// deal with won't enforce that. Since we're hiding object paths we have to figure
// out what we want to do about other encodings. Path/PathBuf are OsString under the
// hood which is right if we want to support that.

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Fd {
    ino: Ino,
    fh: u64,
}

impl Fd {
    pub fn new(ino: Ino, fh: u64) -> Self {
        Self { ino, fh }
    }

    fn add_fh(&self, rhs: u64) -> u64 {
        self.fh.checked_add(rhs).expect("BUG: fd overflow")
    }
}

impl From<Fd> for u64 {
    fn from(fd: Fd) -> Self {
        fd.fh
    }
}

#[derive(Debug)]
enum FileDescriptor {
    Committed {
        key: Arc<object_store::path::Path>,
        range: ByteRange,
    },
    Staged {
        file: tokio::fs::File,
    },
    Static(std::io::Cursor<Arc<[u8]>>),
    Commit,
    ClearCache,
}

pub struct Volume {
    meta: VolumeMetadata,
    version_bytes: Arc<[u8]>,

    cache: Arc<ChunkCache>,
    tempdir: TempDir,
    fds: BTreeMap<Fd, FileDescriptor>,
    store: crate::object_store::RemoteStore,
}

impl Volume {
    pub fn new(
        metadata: VolumeMetadata,
        max_cache_size: u64,
        chunk_size: u64,
        readahead: u64,
        store: crate::object_store::RemoteStore,
    ) -> Self {
        let cache = Arc::new(ChunkCache::new(
            max_cache_size,
            chunk_size,
            store.clone(),
            ReadAheadPolicy { size: readahead },
        ));

        // FIXME
        let tempdir = TempDir::with_prefix("cfs-").unwrap();
        let volume_version =
            std::sync::Arc::from(format!("{:#x}", metadata.version()).into_bytes().as_slice());

        Self {
            tempdir,
            meta: metadata,
            version_bytes: volume_version,
            cache,
            fds: Default::default(),
            store,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.meta.to_bytes()
    }

    fn increment_version(&mut self) -> &[u8] {
        let v = self.meta.increment_version();
        self.version_bytes = Arc::new(v.to_le_bytes());
        &self.version_bytes
    }

    pub fn getattr(&self, ino: Ino) -> Result<&FileAttr> {
        match self.meta.getattr(ino) {
            Some(attr) => Ok(attr),
            None => Err(ErrorKind::NotFound.into()),
        }
    }

    pub fn setattr(
        &mut self,
        ino: Ino,
        mtime: Option<SystemTime>,
        ctime: Option<SystemTime>,
    ) -> Result<&FileAttr> {
        self.meta.setattr(ino, mtime, ctime)
    }

    pub fn modify(
        &mut self,
        ino: Ino,
        location: Option<Location>,
        range: Option<Modify>,
    ) -> Result<()> {
        match ino {
            Ino::CLEAR_CACHE | Ino::COMMIT => Ok(()),
            ino => {
                self.meta.modify(ino, location, range)?;
                Ok(())
            }
        }
    }

    pub fn lookup(&self, parent: Ino, name: &str) -> Result<Option<&FileAttr>> {
        let attr = self.meta.lookup(parent, name)?;
        Ok(attr)
    }

    pub fn mkdir(&mut self, parent: Ino, name: String) -> Result<&FileAttr> {
        self.meta.mkdir(parent, name)
    }

    pub fn rmdir(&mut self, parent: Ino, name: &str) -> Result<()> {
        self.meta.rmdir(parent, name)?;
        Ok(())
    }

    pub fn readdir(&self, ino: Ino) -> Result<impl Iterator<Item = (&str, &FileAttr)>> {
        let iter = self.meta.readdir(ino)?;
        Ok(iter)
    }

    pub fn create(
        &mut self,
        parent: Ino,
        name: String,
        exclusive: bool,
    ) -> Result<(&FileAttr, Fd)> {
        let (path, file) = tempfile(self.tempdir.path());

        let attr = self.meta.create(
            parent,
            name,
            exclusive,
            Location::Staged { path },
            ByteRange::empty(),
        )?;

        let fd = new_fd(&mut self.fds, attr.ino, FileDescriptor::Staged { file });
        Ok((attr, fd))
    }

    pub fn delete(&mut self, parent: Ino, name: &str) -> Result<()> {
        self.meta.delete(parent, name)?;
        Ok(())
    }
}

impl Volume {
    /// Open a Fd to a locally staged file for reading and writing.
    ///
    /// Opening a Fd with write permissions will always truncate the file.
    pub async fn open_read_write(&mut self, ino: Ino) -> Result<Fd> {
        match ino {
            Ino::COMMIT => {
                let fd = new_fd(&mut self.fds, ino, FileDescriptor::Commit);
                Ok(fd)
            }
            Ino::CLEAR_CACHE => {
                let fd = new_fd(&mut self.fds, ino, FileDescriptor::ClearCache);
                Ok(fd)
            }
            Ino::VERSION => Err(ErrorKind::PermissionDenied.into()),
            ino => match self.meta.location(ino) {
                Some((Location::Staged { path }, _)) => {
                    let file = tokio::fs::File::options()
                        .read(true)
                        .write(true)
                        .open(path)
                        .await
                        .map_err(|e| {
                            Error::new_context(e.kind().into(), "failed to open staged file", e)
                        })?;

                    let fd = new_fd(&mut self.fds, ino, FileDescriptor::Staged { file });
                    Ok(fd)
                }
                Some((Location::Committed { .. }, ..)) => {
                    // truncate the file (by assigning it a brand new staged file) if it's
                    // committed. the alternative would be to keep a copy of the committed
                    // file locally as a staged file, which can be expensive if it's a large file.
                    let (path, file) = tempfile(self.tempdir.path());
                    let staged = Location::Staged { path };
                    self.modify(ino, Some(staged), Some(Modify::Set((0, 0).into())))?;

                    let fd = new_fd(&mut self.fds, ino, FileDescriptor::Staged { file });
                    Ok(fd)
                }
                None => Err(ErrorKind::NotFound.into()),
            },
        }
    }

    pub async fn open_read(&mut self, ino: Ino) -> Result<Fd> {
        match ino {
            Ino::VERSION => {
                let fd = new_fd(
                    &mut self.fds,
                    ino,
                    FileDescriptor::Static(std::io::Cursor::new(self.version_bytes.clone())),
                );
                Ok(fd)
            }
            Ino::COMMIT | Ino::CLEAR_CACHE => Err(ErrorKind::PermissionDenied.into()),
            ino => match self.meta.location(ino) {
                Some((Location::Staged { path }, _)) => {
                    let file = tokio::fs::File::open(path).await.map_err(|e| {
                        Error::new_context(e.kind().into(), "failed to open staged file", e)
                    })?;
                    Ok(new_fd(&mut self.fds, ino, FileDescriptor::Staged { file }))
                }
                Some((Location::Committed { key }, range)) => Ok(new_fd(
                    &mut self.fds,
                    ino,
                    FileDescriptor::Committed {
                        key: key.clone(),
                        range: *range,
                    },
                )),
                None => Err(ErrorKind::NotFound.into()),
            },
        }
    }

    pub async fn read_at(&mut self, fd: Fd, offset: u64, buf: &mut [u8]) -> Result<usize> {
        match self.fds.get_mut(&fd) {
            // reads of write-only special fds do nothing
            Some(FileDescriptor::ClearCache) | Some(FileDescriptor::Commit) => Ok(0),
            // static bytes just use their cursor
            Some(FileDescriptor::Static(cursor)) => Ok({
                read_at(cursor, offset, buf)
                    .await
                    .expect("BUG: invalid read from metadata")
            }),
            Some(FileDescriptor::Committed { key, range }) => {
                // FIXME: readahead needs to know the extent of the location -
                // the range here only includes the extent of THIS file in the
                // total blob. without knowing the full range we can TRY to prefetch
                // into the next chunk but we'll only get one at most - that banks
                // on the object store's API being kind enough to return partial ranges.
                let read_len = std::cmp::min(range.len, buf.len() as u64);
                let blob_offset = range.offset + offset;
                let bytes: Vec<Bytes> = self.cache.get_at(key, blob_offset, read_len).await?;
                Ok(copy_into(buf, &bytes))
            }
            Some(FileDescriptor::Staged { file, .. }) => read_at(file, offset, buf)
                .await
                .map_err(|e| Error::new_context(e.kind().into(), "failed to read staged file", e)),
            None => Err(ErrorKind::NotFound.into()),
        }
    }

    pub async fn write_at(&mut self, fd: Fd, offset: u64, data: &[u8]) -> Result<usize> {
        match self.fds.get_mut(&fd) {
            Some(FileDescriptor::ClearCache) => {
                self.cache.clear();
                Ok(data.len())
            }
            Some(FileDescriptor::Commit) => {
                self.commit().await?;
                Ok(data.len())
            }
            // write directly into a staged file
            Some(FileDescriptor::Staged { file, .. }) => {
                let n = write_at(file, offset, data).await.map_err(|e| {
                    let kind = e.kind().into();
                    Error::new_context(kind, "failed to write staged file", e)
                })?;
                self.modify(fd.ino, None, Some(Modify::Max(offset + n as u64)))?;
                Ok(n)
            }
            // no other fds are writable
            Some(_) => Err(ErrorKind::PermissionDenied.into()),
            None => Err(ErrorKind::NotFound.into()),
        }
    }

    pub async fn release(&mut self, fd: Fd) -> Result<()> {
        match self.fds.remove(&fd) {
            Some(_) => Ok(()),
            None => Err(ErrorKind::NotFound.into()),
        }
    }
}

async fn read_at<R: AsyncRead + AsyncSeek + Unpin>(
    file: &mut R,
    offset: u64,
    buf: &mut [u8],
) -> std::io::Result<usize> {
    file.seek(SeekFrom::Start(offset)).await?;
    let n = file.read(buf).await?;
    Ok(n)
}

async fn write_at<W: AsyncWrite + AsyncSeek + Unpin>(
    file: &mut W,
    offset: u64,
    buf: &[u8],
) -> std::io::Result<usize> {
    file.seek(SeekFrom::Start(offset)).await?;
    let n = file.write(buf).await?;
    Ok(n)
}

fn copy_into(mut buf: &mut [u8], bytes: &[Bytes]) -> usize {
    let mut copied = 0;
    for bs in bytes {
        let n = std::cmp::min(buf.len(), bs.len());
        buf[..n].copy_from_slice(&bs[..n]);
        copied += n;
        buf = &mut buf[n..];
    }
    copied
}

// FIXME: this needs to allocate and check for remaining fds instead of just
// trying to increment every time and crashing. it's u64 so we probably won't
// hit it for a while but that's jank
fn new_fd(fd_set: &mut BTreeMap<Fd, FileDescriptor>, ino: Ino, d: FileDescriptor) -> Fd {
    let next_fh = fd_set.keys().last().cloned().unwrap_or_default().add_fh(1);
    let fd = Fd { ino, fh: next_fh };
    fd_set.insert(fd, d);
    fd
}

// FIXME: handle errors
fn tempfile(tempdir: &std::path::Path) -> (PathBuf, tokio::fs::File) {
    let f = tempfile::Builder::new()
        .disable_cleanup(true)
        .tempfile_in(tempdir)
        .unwrap();

    let (f, path) = f.into_parts();
    (path.to_path_buf(), tokio::fs::File::from_std(f))
}

impl Volume {
    async fn commit(&mut self) -> Result<()> {
        let mut staged = StagedVolume { inner: self };
        let (dest, ranges) = staged.upload().await?;
        staged.modify(dest, ranges)?;
        staged.persist().await?;

        Ok(())
    }
}

struct StagedVolume<'a> {
    inner: &'a mut Volume,
}

impl StagedVolume<'_> {
    /// Upload all staged files into a single blob under base.
    ///
    /// Returns the location of the newly uploaded blob and a vector that maps each newly written
    /// Ino to its ByteRange within the new blob.
    async fn upload(&self) -> Result<(Location, Vec<(Ino, ByteRange)>)> {
        macro_rules! try_mpu {
            ($mpu:expr, $context:literal) => {
                $mpu.await.map_err(|e| {
                    let kind = match &e {
                        object_store::Error::InvalidPath { .. } => ErrorKind::InvalidData,
                        object_store::Error::PermissionDenied { .. } => ErrorKind::PermissionDenied,
                        object_store::Error::Unauthenticated { .. } => ErrorKind::PermissionDenied,
                        _ => ErrorKind::Other,
                    };
                    Error::new_context(kind, $context, e)
                })?
            };
        }

        let dest = self.inner.store.new_data();

        let mut offset = 0;
        let mut staged = Vec::new();
        let mut writer = try_mpu!(
            self.inner.store.client.put_multipart(&dest),
            "failed to start multipart upload"
        );

        for (attr, path) in self.inner.meta.iter_staged() {
            let data = tokio::fs::read(path)
                .await
                .map_err(|e| Error::new_context(e.kind().into(), "failed to read staged file", e))?
                .into();

            try_mpu!(
                writer.put_part(PutPayload::from_bytes(data)),
                "failed to upload part"
            );

            staged.push((attr.ino, (offset, attr.size).into()));
            offset += attr.size;
        }
        try_mpu!(writer.complete(), "failed to complete upload");

        Ok((Location::committed(dest), staged))
    }

    /// Relocate all staged files to dest.
    fn modify(&mut self, dest: Location, ranges: Vec<(Ino, ByteRange)>) -> Result<()> {
        for (ino, byte_range) in ranges {
            self.inner
                .meta
                .modify(ino, Some(dest.clone()), Some(Modify::Set(byte_range)))?;
        }

        // deduplicate and clean up all hanging staged Locations
        self.inner.meta.clean_staged_locations(dest);

        Ok(())
    }

    /// Mint and upload a new version of Volume.
    async fn persist(self) -> Result<()> {
        self.inner.increment_version();
        let version = self.inner.meta.version();

        let new_volume = bytes::Bytes::from(self.inner.to_bytes()?);
        let res = self
            .inner
            .store
            .client
            .put_opts(
                &self.inner.store.metadata(version),
                PutPayload::from_bytes(new_volume),
                PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                },
            )
            .await;

        match res {
            Ok(_) => Ok(()),
            Err(e) => {
                let kind = match e {
                    object_store::Error::InvalidPath { .. } => ErrorKind::InvalidData,
                    object_store::Error::AlreadyExists { .. } => ErrorKind::AlreadyExists,
                    object_store::Error::PermissionDenied { .. } => ErrorKind::PermissionDenied,
                    object_store::Error::Unauthenticated { .. } => ErrorKind::PermissionDenied,
                    _ => ErrorKind::Other,
                };
                Err(Error::new_context(
                    kind,
                    "uploading volume metadata failed",
                    e,
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    #[test]
    fn test_copy_into() {
        // buf.len() = sum(bytes.len())
        {
            let mut buf = [0u8; 10];
            let bytes = &[
                Bytes::from_static(b"123"),
                Bytes::from_static(b"456"),
                Bytes::from_static(b"7"),
                Bytes::from_static(b"890"),
            ];
            assert_eq!(10, copy_into(&mut buf, bytes));
            assert_eq!(&buf[0..10], b"1234567890");
        }
        // buf.len() > sum(bytes.len())
        {
            let mut buf = [0u8; 10];
            let bytes = &[Bytes::from_static(b"123"), Bytes::from_static(b"456")];
            assert_eq!(6, copy_into(&mut buf, bytes));
            assert_eq!(&buf[0..6], b"123456");
        }
        // buf.len() < sum(bytes.len())
        {
            let mut buf = [0u8; 5];
            let bytes = &[
                Bytes::from_static(b"123"),
                Bytes::from_static(b"456"),
                Bytes::from_static(b"7"),
                Bytes::from_static(b"890"),
            ];
            assert_eq!(5, copy_into(&mut buf, bytes));
            assert_eq!(&buf[0..5], b"12345");
        }
    }

    async fn write(volume: &mut Volume, fd: Fd, contents: &'static str) {
        volume.write_at(fd, 0, contents.as_bytes()).await.unwrap();
        volume.release(fd).await.unwrap();
    }

    async fn assert_read(volume: &mut Volume, name: &'static str, contents: &'static str) {
        let attr = volume.lookup(Ino::Root, name).unwrap().unwrap();
        let mut buf = vec![0u8; attr.size as usize];
        let fd = volume.open_read(attr.ino).await.unwrap();
        let n = volume.read_at(fd, 0, &mut buf).await.unwrap();
        assert_eq!(n, contents.len());
        assert_eq!(buf, contents.as_bytes());
    }

    #[tokio::test]
    async fn commit() {
        let root = tempfile::tempdir().expect("tempdir");
        let store_root = root.path().join("store");
        std::fs::create_dir_all(&store_root).unwrap();

        let client =
            crate::object_store::RemoteStore::from_str(&format!("file://{}", store_root.display()))
                .unwrap();

        let mut volume = Volume::new(VolumeMetadata::empty(), 1024, 1024, 1024, client.clone());
        let version = volume.meta.version();

        // clean volume -- this is not staged
        assert!(!volume.meta.is_staged());

        // creating two files, it should be a staged volume now.
        let (attr1, fd1) = volume.create(Ino::Root, "hello.txt".into(), true).unwrap();
        let attr1 = attr1.clone();
        write(&mut volume, fd1, "hello").await;
        let (attr2, fd2) = volume.create(Ino::Root, "world.txt".into(), true).unwrap();
        let attr2 = attr2.clone();
        write(&mut volume, fd2, "world").await;

        assert!(volume.meta.is_staged());
        for attr in [&attr1, &attr2] {
            assert!(matches!(
                volume.meta.location(attr.ino),
                Some((Location::Staged { .. }, _))
            ));
        }

        // commit!!!
        let commit_fd = volume.open_read_write(Ino::COMMIT).await.unwrap();
        write(&mut volume, commit_fd, "1").await;

        // after commit, both files are no longer staged
        assert!(!volume.meta.is_staged());
        for attr in [&attr1, &attr2] {
            assert!(matches!(
                volume.meta.location(attr.ino),
                Some((Location::Committed { .. }, _))
            ));
        }

        // read the new volume and assert that the previously staged files have the right
        // contents, and the version is bumped
        let metadata = client.metadata(version + 1);
        let metadata_bytes = client
            .client
            .get(&metadata)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        let metadata = VolumeMetadata::from_bytes(&metadata_bytes).unwrap();
        let mut volume = Volume::new(metadata, 1024, 1024, 1024, client);
        assert_eq!(volume.meta.version(), version + 1);

        assert_read(&mut volume, "hello.txt", "hello").await;
        assert_read(&mut volume, "world.txt", "world").await;
    }
}
