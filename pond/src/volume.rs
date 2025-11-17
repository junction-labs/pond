use futures::future::BoxFuture;
use futures::{FutureExt, Stream};
use pond_core::FileAttr;
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::adapter::OpenOptions;
use crate::adapter::VolumeAdapter;
use crate::file::File;
use crate::path::Path;

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
    pub Metadata => metadata(path: crate::path::Path) -> pond_core::Result<FileAttr>,
    /// Checks if the provided path exists. Returns `Ok(true)` if the path points to an existing
    /// entity.
    pub Exists => exists(path: crate::path::Path) -> pond_core::Result<bool>,
    /// Checks if the provided path exists and is staged. Returns `Ok(true)` if the path points
    /// to an existing entity and that entity is staged.
    pub IsStaged => is_staged(path: crate::path::Path) -> pond_core::Result<bool>,
    /// Creates a new, empty directory at the provided path.
    pub CreateDir => create_dir(path: crate::path::Path) -> pond_core::Result<FileAttr>,
    /// Recursively create a directory and all of its parent components if they are missing.
    ///
    /// This operation is not atomic. On error, any parent components created will remain.
    pub CreateDirAll => create_dir_all(path: crate::path::Path) -> pond_core::Result<FileAttr>,
    /// Removes an empty directory.
    ///
    /// To remove an non-empty directory (and all of its contents), use [`Volume::remove_dir_all`].
    pub RemoveDir => remove_dir(path: crate::path::Path) -> pond_core::Result<()>,
    /// Removes the directory at this path, after removing all its contents.
    pub RemoveDirAll => remove_dir_all(path: crate::path::Path) -> pond_core::Result<()>,
    /// Removes a file.
    pub RemoveFile => remove_file(path: crate::path::Path) -> pond_core::Result<()>,
    pub Copy => copy(src: crate::path::Path, dst: crate::path::Path) -> pond_core::Result<()>,
    /// Renames a file or directory to a new name. For files, this will replace the original file if
    /// `dst` already exists. For directories, `dst` must not exist or be an empty directory.
    ///
    /// This renames within the Volume, it does not cross Volume boundaries.
    pub Rename => rename(src: crate::path::Path, dst: crate::path::Path) -> pond_core::Result<()>,
    /// Creates an empty file if it doesn't exist, otherwise it updates the ctime of the existing
    /// file.
    pub Touch => touch(path: crate::path::Path) -> pond_core::Result<FileAttr>,
    pub(crate) ReadDirPage => read_dir_page(path: crate::path::Path, offset: Option<String>, len: NonZeroUsize) -> pond_core::Result<Vec<crate::DirEntry>>,
    pub(crate) OpenFd => open(path: crate::path::Path, option: OpenOptions) -> pond_core::Result<(pond_core::Fd, FileAttr)>,
    pub(crate) ReleaseFd => release(fd: pond_core::Fd) -> pond_core::Result<()>,
    pub(crate) ReadAt => read_at(fd: pond_core::Fd, offset: u64, size: usize) -> pond_core::Result<bytes::Bytes>,
    pub(crate) WriteAt => write_at(fd: pond_core::Fd, offset: u64, buf: bytes::Bytes) -> pond_core::Result<usize>,
}

impl Volume {
    /// Reads directory entries as a stream of [`crate::DirEntry`] values.
    ///
    /// Lazily reads the directories in pages. Does not start reading pages until the first call to
    /// ReadDir::next() is made.
    pub fn read_dir(&self, path: crate::path::Path) -> pond_core::Result<ReadDir> {
        Ok(ReadDir::new(self.clone(), path))
    }

    /// Open or create a file according to the given OpenOptions.
    pub async fn open(
        &self,
        path: crate::path::Path,
        option: OpenOptions,
    ) -> pond_core::Result<File> {
        let (fd, attr) = self.open_fd(path, option).await?;
        Ok(File::new(fd, attr, self.clone()))
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

/// Stream of directory entries retrieved from a [`Volume`].
///
/// This stream fetches entries in batches from the backend and yields them one by one.
pub struct ReadDir {
    volume: Volume,
    path: Path,
    buffer: VecDeque<crate::DirEntry>,
    offset: Option<String>,
    finished: bool,
    inflight: Option<BoxFuture<'static, pond_core::Result<Vec<crate::DirEntry>>>>,
}

impl ReadDir {
    const PAGESIZE: NonZeroUsize = std::num::NonZero::new(32).unwrap();

    fn new(volume: Volume, path: Path) -> Self {
        Self {
            volume,
            path,
            buffer: VecDeque::new(),
            offset: None,
            finished: false,
            inflight: None,
        }
    }
}

impl Stream for ReadDir {
    type Item = pond_core::Result<crate::DirEntry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // stream is already exhausted.
        if self.finished {
            return Poll::Ready(None);
        }

        // there're still entries within our buffer
        if let Some(entry) = self.buffer.pop_front() {
            return Poll::Ready(Some(Ok(entry)));
        }

        // fetch more entries
        match self.inflight.as_mut() {
            Some(fut) => match fut.poll_unpin(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(entries)) => {
                    self.inflight = None;

                    if entries.is_empty() {
                        self.finished = true;
                        return Poll::Ready(None);
                    }

                    self.offset = entries.last().map(|e| e.file_name().to_string());
                    self.buffer = entries.into();
                    // return one of the entries
                    let entry = self.buffer.pop_front().expect("batch should be non-empty");
                    Poll::Ready(Some(Ok(entry)))
                }
                Poll::Ready(Err(e)) => {
                    self.inflight = None;
                    self.finished = true;
                    Poll::Ready(Some(Err(e)))
                }
            },
            None => {
                let volume = self.volume.clone();
                let path = self.path.clone();
                let offset = self.offset.take();
                self.inflight = Some(
                    async move { volume.read_dir_page(path, offset, Self::PAGESIZE).await }.boxed(),
                );
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}
