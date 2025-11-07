use pond::FileAttr;
use tokio::sync::{mpsc, oneshot};
use volume::VolumeAdapter;

mod file;
mod volume;

pub use volume::{OpenMode, OpenOptions};

use crate::file::ReadOnlyFile;

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
        let path_volume = crate::volume::VolumeAdapter::new(volume);

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
                        $($arg_name: $arg_ty),*
                    ) -> $result_ty {
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
    /// Get the version of the current Volume.
    pub Version => version() -> pond::Version,
    /// Commit and persist all currently staged changes with the provided version label.
    pub Commit  => commit(version: String) -> Result<(), pond::Error>,
    /// Get the FileAttr for the directory or file at the given path.
    pub Metadata => metadata(path: String) -> pond::Result<FileAttr>,
    /// Check if the given path exists.
    pub Exists => exists(path: String) -> pond::Result<bool>,
    /// ReadDir
    pub ReadDir => read_dir(path: String) -> pond::Result<()>,
    /// CreateDir
    pub CreateDir => create_dir(path: String) -> pond::Result<FileAttr>,
    pub CreateDirAll => create_dir_all(path: String) -> pond::Result<FileAttr>,
    pub RemoveDir => remove_dir(path: String) -> pond::Result<()>,
    pub RemoveDirAll => remove_dir_all(path: String) -> pond::Result<()>,
    pub RemoveFile => remove_file(path: String) -> pond::Result<()>,
    pub Copy => copy(src: String, dst: String) -> pond::Result<()>,
    pub Rename => rename(src: String, dst: String) -> pond::Result<()>,
    pub Touch => touch(path: String) -> pond::Result<FileAttr>,
    OpenFd => open(path: String, options: OpenOptions) -> pond::Result<(pond::Fd, FileAttr)>,
    ReadAt => read_at(fd: pond::Fd, offset: u64, size: usize) -> pond::Result<bytes::Bytes>,
}

impl Volume {
    pub async fn open(&self, path: String, options: OpenOptions) -> pond::Result<ReadOnlyFile> {
        let (fd, attr) = self.open_fd(path, options).await?;
        Ok(ReadOnlyFile::new(fd, attr, self.clone()))
    }
}
