#![allow(unused)]

use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};

// TODO: We're starting with a Reader/Writer split for modifying files in a volume
// to make it clear that you're either getting an input or output stream, not doing
// random writes. If that changes we should switch to something more like a File
// struct at write_at/pread(2) methods.

// TODO: We should use our own Path type abstraction here. std::fs::Path
// is pretty close to right but offers a bunch of system methods (like canonicalize)
// that mean it will do unexpected things if we use it as part of our interface.
//
// Both AWS and GCS commit to key names being valid utf8 but random filesystems we
// deal with won't enforce that. Since we're hiding object paths we have to figure
// out what we want to do about other encodings. Path/PathBuf are OsString under the
// hood which is right if we want to support that.

#[derive(Debug, thiserror::Error)]
#[error("something went wrong")]
pub struct Error {}

#[derive(Debug, Clone, Copy)]
pub enum FileType {
    File,
    Directory,
    SymLink,
}

pub struct DirEntry {}

impl DirEntry {
    pub fn name(&self) -> &str {
        unimplemented!()
    }

    pub fn file_type(&self) -> FileType {
        unimplemented!()
    }

    pub fn path(&self) -> &str {
        unimplemented!()
    }
}

/// A CoolFS client that provides access to a cluster and all of the volumes
/// it contains.
///
/// ```no_run
/// # use cfs_client::Client;
/// # use tokio::io::{AsyncRead, AsyncReadExt};
/// async fn process_data<R: AsyncRead>(r: R) { /* do stuff here */ }
///
/// # async fn doc() {
/// let client = Client::new();
///
/// assert_eq!(client.volumes().await.unwrap(), vec!["now_thats_what_i_call_data"]);
///
/// let volume = client
///     .mount("now_thats_what_i_call_data", "vol3")
///     .await
///     .unwrap();
///
/// let reader = volume.read("/data/all_star.hdf5").await.unwrap();
/// process_data(reader);
/// # }
/// ```
pub struct Client {}

impl Client {
    #[deprecated = "stub method"]
    pub fn new() -> Self {
        unimplemented!()
    }

    /// List all available volumes.
    ///
    /// *TODO*: This should return volume metadata.
    pub async fn volumes(&self) -> Result<Vec<String>, Error> {
        unimplemented!()
    }

    /// List all versions of a specific volume.
    ///
    /// *TODO*: This should return volume-version metadata.
    pub async fn versions(&self, volume: &str) -> Result<Vec<String>, Error> {
        unimplemented!()
    }

    /// Load a volume at a specific version. Returns an error if the
    /// volume doesn't exist or can't be loaded.
    pub async fn mount(
        &self,
        volume: impl AsRef<str>,
        version: impl AsRef<str>,
    ) -> Result<Volume, Error> {
        unimplemented!()
    }

    // TODO: should we have read-only ops here too? ls, cat, etc?
}

/// An CoolFS volume at a specific version.
///
/// Changes to a volume are staged locally, and not written back until
/// [commit](Self::commit) is called. Writes can be read locally
pub struct Volume {}

impl Volume {
    /// List directory contents.
    pub async fn ls(&self, path: impl AsRef<str>) -> Result<ReadDir, Error> {
        unimplemented!()
    }

    /// Open a file for reading.
    ///
    /// Directories cannot be opened, use `ls` to list the contents of a directory.
    pub async fn read(&self, path: impl AsRef<str>) -> Result<Reader, Error> {
        unimplemented!()
    }

    /// Open a file for writing.
    ///
    /// Opening a file for writing replaces the contents of the file entirely, and
    /// does not preserve any existing content.
    pub async fn write(&mut self, path: impl AsRef<str>) -> Result<Writer, Error> {
        unimplemented!()
    }

    /// Copy a file into CoolFS from the local filesystem.
    ///
    /// This is logically equivalent to opening a writer with `write` and
    /// calling `tokio::io::copy` to write the bytes into the file but is
    /// specialized to avoid making an extra copy of the data.
    pub async fn write_file(
        &mut self,
        path: impl AsRef<str>,
        local_path: impl AsRef<std::path::Path>,
    ) -> Result<(), Error> {
        unimplemented!()
    }

    /// Create a directory.
    pub async fn mkdir(&mut self, path: impl AsRef<str>) -> Result<(), Error> {
        unimplemented!()
    }

    /// Create a directory and any intermediate directories as required.
    pub async fn mkdir_all(&mut self, path: impl AsRef<str>) -> Result<(), Error> {
        unimplemented!()
    }

    /// Remove a directory.
    ///
    /// Directories must be empty before being removed.
    pub async fn rmdir(&mut self, path: impl AsRef<str>) {
        unimplemented!()
    }

    /// Check to see if a volume has staged changes that have not yet been comitted.
    pub async fn has_staged_changes(&self) -> bool {
        unimplemented!()
    }

    /// Save all staged writes to the current volume. Returns the generated
    /// name of the new version once all of the changes have been persisted.
    pub async fn commit(&mut self) -> Result<String, Error> {
        unimplemented!()
    }
}

pub struct ReadDir;

/// FIXME: should this be an async iterator/stream/etc or is sync okay?
impl Iterator for ReadDir {
    type Item = DirEntry;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

pub struct Reader {}

impl AsyncRead for Reader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        todo!()
    }
}

impl AsyncSeek for Reader {
    fn start_seek(
        self: std::pin::Pin<&mut Self>,
        position: std::io::SeekFrom,
    ) -> std::io::Result<()> {
        todo!()
    }

    fn poll_complete(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        todo!()
    }
}

pub struct Writer {}

impl AsyncWrite for Writer {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        todo!()
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        todo!()
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        todo!()
    }
}

impl AsyncSeek for Writer {
    fn start_seek(
        self: std::pin::Pin<&mut Self>,
        position: std::io::SeekFrom,
    ) -> std::io::Result<()> {
        todo!()
    }

    fn poll_complete(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        todo!()
    }
}
