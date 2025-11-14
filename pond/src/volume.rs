use pond_core::FileAttr;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::adapter::VolumeAdapter;
use crate::file::File;
use crate::file::OpenOptions;

#[derive(Clone)]
pub struct Volume {
    cmds: mpsc::Sender<Cmd>,
}

impl Volume {
    pub async fn load(
        volume: impl AsRef<str>,
        version: Option<pond_core::Version>,
        cache_config: pond_core::CacheConfig,
    ) -> pond_core::Result<Self> {
        let volume = pond_core::Client::open(volume)?
            .with_cache_config(cache_config)
            .load_volume(&version)
            .await?;
        let path_volume = crate::adapter::VolumeAdapter::new(volume);

        // interfacing with pond_core::Volume through a channel, following the Communicating Sequential
        // Processes (CSP) pattern. this serializes all operations to pond_core::Volume making it
        // thread-safe (which pond_core::Volume isn't).
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
/// model to make pond_core::Volume thread-safe.
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
    pub Version => version() -> pond_core::Version,
    /// Commits and persists staged changes with the provided version label.
    pub Commit  => commit(version: String) -> Result<(), pond_core::Error>,
    /// Queries metadata about the underlying the directory or file.
    pub Metadata => metadata(path: String) -> pond_core::Result<FileAttr>,
    /// Checks if the provided path exists. Returns `Ok(true)` if the path points to an existing
    /// entity.
    pub Exists => exists(path: String) -> pond_core::Result<bool>,
    /// Checks if the provided path exists and is staged. Returns `Ok(true)` if the path points
    /// to an existing entity and that entity is staged.
    pub IsStaged => is_staged(path: String) -> pond_core::Result<bool>,
    /// Reads the contents of a directory. Returns a snapshot (vector of entries) of the directory contents.
    ///
    /// Takes an optional offset and length for paginated reads, since the contents of the
    /// directory can be large. The offset is the filename of the last file you received from
    /// this function.
    pub ReadDir => read_dir(path: String, offset: Option<String>, len: usize) -> pond_core::Result<Vec<crate::DirEntry>>,
    /// Creates a new, empty directory at the provided path.
    pub CreateDir => create_dir(path: String) -> pond_core::Result<FileAttr>,
    /// Recursively create a directory and all of its parent components if they are missing.
    ///
    /// This operation is not atomic. On error, any parent components created will remain.
    pub CreateDirAll => create_dir_all(path: String) -> pond_core::Result<FileAttr>,
    /// Removes an empty directory.
    ///
    /// To remove an non-empty directory (and all of its contents), use [`Volume::remove_dir_all`].
    pub RemoveDir => remove_dir(path: String) -> pond_core::Result<()>,
    /// Removes the directory at this path, after removing all its contents.
    pub RemoveDirAll => remove_dir_all(path: String) -> pond_core::Result<()>,
    /// Removes a file.
    pub RemoveFile => remove_file(path: String) -> pond_core::Result<()>,
    pub Copy => copy(src: String, dst: String) -> pond_core::Result<()>,
    /// Renames a file or directory to a new name. For files, this will replace the original file if
    /// `dst` already exists. For directories, `dst` must not exist or be an empty directory.
    ///
    /// This renames within the Volume, it does not cross Volume boundaries.
    pub Rename => rename(src: String, dst: String) -> pond_core::Result<()>,
    /// Creates an empty file if it doesn't exist, otherwise it updates the ctime of the existing
    /// file.
    pub Touch => touch(path: String) -> pond_core::Result<FileAttr>,
    pub(crate) Create => create(path: String) -> pond_core::Result<(pond_core::Fd, FileAttr)>,
    pub(crate) Truncate => truncate(path: String, len: u64) -> pond_core::Result<()>,
    pub(crate) OpenRoFd => open_read(path: String) -> pond_core::Result<(pond_core::Fd, FileAttr)>,
    pub(crate) OpenRwFd => open_read_write(path: String) -> pond_core::Result<(pond_core::Fd, FileAttr)>,
    pub(crate) ReleaseFd => release(fd: pond_core::Fd) -> pond_core::Result<()>,
    pub(crate) ReadAt => read_at(fd: pond_core::Fd, offset: u64, size: usize) -> pond_core::Result<bytes::Bytes>,
    pub(crate) WriteAt => write_at(fd: pond_core::Fd, offset: u64, buf: bytes::Bytes) -> pond_core::Result<usize>,
}

impl Volume {
    pub async fn open(&self, path: String, option: OpenOptions) -> pond_core::Result<File> {
        let (fd, attr) = if option.write {
            if option.create {
                self.create(path).await?
            } else {
                let staged = self.is_staged(path.clone()).await?;
                if !staged && !option.truncate {
                    return Err(pond_core::Error::new(
                        pond_core::ErrorKind::Unsupported,
                        "Committed files cannot be open for writing without truncating.",
                    ));
                } else if staged && option.truncate {
                    self.truncate(path.clone(), 0u64).await?;
                }
                // !staged && option.truncate is done by default in the lower level volume. no need
                // to explicitly call truncate.

                self.open_rw_fd(path).await?
            }
        } else {
            self.open_ro_fd(path).await?
        };

        Ok(File::new(fd, attr, self.clone()))
    }

    /// Opens a file for reading.
    ///
    /// If the file is committed, the file is truncated. If the file is staged, there's no
    /// truncating. For more granular options, use [`Volume::open`] which takes an [`OpenOptions`].
    pub async fn open_read(&self, path: String) -> pond_core::Result<File> {
        self.open(path, OpenOptions::new().write(false)).await
    }

    /// Opens a file for reading and writing.
    ///
    /// If the file is committed, the file is truncated. If the file is staged, there's no
    /// truncating. For more granular options, use [`Volume::open`] which takes an [`OpenOptions`].
    pub async fn open_read_write(&self, path: String) -> pond_core::Result<File> {
        let committed = !self.is_staged(path.clone()).await?;
        self.open(path, OpenOptions::new().write(true).truncate(committed))
            .await
    }

    /// Try to send a ReleaseFd through the Cmd channel without blocking.
    ///
    /// If the channel is full, return the Fd within the error of the Result.
    pub(crate) fn try_release_fd(&self, fd: pond_core::Fd) -> Result<(), pond_core::Fd> {
        let (tx, _rx) = oneshot::channel();
        let cmd = Cmd::ReleaseFd { fd, resp: tx };
        match self.cmds.try_send(cmd) {
            Ok(()) => Ok(()),
            Err(_) => Err(fd),
        }
    }
}
