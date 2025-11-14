use pond::FileAttr;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::adapter::VolumeAdapter;
use crate::file::ReadOnlyFile;
use crate::file::ReadWriteFile;

#[derive(Clone)]
pub struct Volume {
    cmds: mpsc::Sender<Cmd>,
}

impl Volume {
    pub async fn load(
        volume: impl AsRef<str>,
        version: Option<pond::Version>,
        cache_config: pond::CacheConfig,
    ) -> pond::Result<Self> {
        let volume = pond::Client::open(volume)?
            .with_cache_config(cache_config)
            .load_volume(&version)
            .await?;
        let path_volume = crate::adapter::VolumeAdapter::new(volume);

        // interfacing with pond::Volume through a channel, following the Communicating Sequential
        // Processes (CSP) pattern. this serializes all operations to pond::Volume making it
        // thread-safe (which pond::Volume isn't).
        let (tx, rx) = mpsc::channel(16);
        let _handle = tokio::spawn(dispatch_loop(path_volume, rx));

        Ok(Self { cmds: tx })
    }
}

/// Process and dispatch Cmds on the receiving end of the mpsc::channel.
async fn dispatch_loop(mut volume: VolumeAdapter, mut cmds: mpsc::Receiver<Cmd>) {
    while let Some(cmd) = cmds.recv().await {
        dispatch(&mut volume, cmd).await;
    }
}

/// Generates some boilerplate needed to support the communicating sequential processes (CSP)
/// model to make pond::Volume thread-safe.
macro_rules! cmds {
    ($(
        $(#[$id_attr:meta])* $vis:vis $variant:ident => $method:ident(
            $($arg_name:ident : $arg_ty:ty),* $(,)?
        ) -> $result_ty:ty
    ),* $(,)?) => {

        enum Cmd {
            $(
                $variant {
                    $(
                        $arg_name: $arg_ty,
                    )*
                    resp: oneshot::Sender<$result_ty>,
                }
            ),*
        }

        async fn dispatch(volume: &mut VolumeAdapter, cmd: Cmd) {
            match cmd {
                $(
                    Cmd::$variant { $($arg_name,)* resp } => {
                        let _ = resp.send(volume.$method($($arg_name),*).await);
                    }
                ),*
            }
        }

        impl Volume {
            $(
                paste::paste! {
                    $(#[$id_attr])*
                    $vis async fn [<$variant:snake>](
                        &self,
                        $($arg_name: impl Into<$arg_ty>),*
                    ) -> $result_ty {
                        $(
                            let $arg_name = $arg_name.into();
                        )*
                        let (tx, rx) = oneshot::channel();
                        let cmd = Cmd::$variant {
                            $(
                                $arg_name,
                            )*
                            resp: tx,
                        };
                        let _ = self.cmds.send(cmd).await;
                        rx.await.expect("volume exited unexpectedly")
                    }
                }
            )*
        }
    };
}

cmds! {
    /// Gets the version of the currently loaded Volume.
    pub Version => version() -> pond::Version,
    /// Commits and persists staged changes with the provided version label.
    pub Commit  => commit(version: String) -> Result<(), pond::Error>,
    /// Queries metadata about the underlying the directory or file.
    pub Metadata => metadata(path: String) -> pond::Result<FileAttr>,
    /// Checks if the provided path exists. Returns `Ok(true)` if the path points to an existing
    /// entity.
    pub Exists => exists(path: String) -> pond::Result<bool>,
    /// Reads the contents of a directory. Returns a snapshot (vector of entries) of the directory contents.
    ///
    /// Takes an optional offset and length for paginated reads, since the contents of the
    /// directory can be large. The offset is the filename of the last file you received from
    /// this function.
    pub ReadDir => read_dir(path: String, offset: Option<String>, len: usize) -> pond::Result<Vec<crate::DirEntry>>,
    /// Creates a new, empty directory at the provided path.
    pub CreateDir => create_dir(path: String) -> pond::Result<FileAttr>,
    /// Recursively create a directory and all of its parent components if they are missing.
    ///
    /// This operation is not atomic. On error, any parent components created will remain.
    pub CreateDirAll => create_dir_all(path: String) -> pond::Result<FileAttr>,
    /// Removes an empty directory.
    ///
    /// To remove an non-empty directory (and all of its contents), use [`remove_dir_all`].
    pub RemoveDir => remove_dir(path: String) -> pond::Result<()>,
    /// Removes the directory at this path, after removing all its contents.
    pub RemoveDirAll => remove_dir_all(path: String) -> pond::Result<()>,
    /// Removes a file.
    pub RemoveFile => remove_file(path: String) -> pond::Result<()>,
    pub Copy => copy(src: String, dst: String) -> pond::Result<()>,
    /// Renames a file or directory to a new name. For files, this will replace the original file if
    /// `dst` already exists. For directories, `dst` must not exist or be an empty directory.
    ///
    /// This renames within the Volume, it does not cross Volume boundaries.
    pub Rename => rename(src: String, dst: String) -> pond::Result<()>,
    /// Creates an empty file if it doesn't exist, otherwise it updates the ctime of the existing
    /// file.
    pub Touch => touch(path: String) -> pond::Result<FileAttr>,
    pub(crate) OpenRoFd => open_read(path: String) -> pond::Result<(pond::Fd, FileAttr)>,
    pub(crate) OpenRwFd => open_read_write(path: String) -> pond::Result<(pond::Fd, FileAttr)>,
    pub(crate) ReleaseFd => release(fd: pond::Fd) -> pond::Result<()>,
    pub(crate) ReadAt => read_at(fd: pond::Fd, offset: u64, size: usize) -> pond::Result<bytes::Bytes>,
    pub(crate) WriteAt => write_at(fd: pond::Fd, offset: u64, buf: bytes::Bytes) -> pond::Result<usize>,
}

impl Volume {
    /// Opens a file for reading.
    pub async fn open_read(&self, path: String) -> pond::Result<ReadOnlyFile> {
        let (fd, attr) = self.open_ro_fd(path).await?;
        Ok(ReadOnlyFile::new(fd, attr, self.clone()))
    }

    /// Opens a file for reading and writing.
    ///
    /// If the file is committed, this truncates the file completely, creating a new file.
    pub async fn open_read_write(&self, path: String) -> pond::Result<ReadWriteFile> {
        let (fd, attr) = self.open_rw_fd(path).await?;
        Ok(ReadWriteFile::new(fd, attr, self.clone()))
    }

    /// Try to send a ReleaseFd through the Cmd channel without blocking.
    ///
    /// If the channel is full, return the Fd within the error of the Result.
    pub(crate) fn try_release_fd(&self, fd: pond::Fd) -> Result<(), pond::Fd> {
        let (tx, _rx) = oneshot::channel();
        let cmd = Cmd::ReleaseFd { fd, resp: tx };
        match self.cmds.try_send(cmd) {
            Ok(()) => Ok(()),
            Err(_) => Err(fd),
        }
    }
}
