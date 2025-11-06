use pond::FileAttr;
use tokio::sync::{mpsc, oneshot};

use crate::cmd::Cmd;

mod cmd;
mod volume;

pub struct Volume {
    cmds: mpsc::Sender<Cmd>,
}

impl Volume {
    pub async fn load(
        volume: impl AsRef<str>,
        version: Option<pond::Version>,
        cache_config: pond::CacheConfig,
    ) -> pond::Result<Self> {
        let volume = pond::Client::new(volume)?
            .with_cache_config(cache_config)
            .load_volume(&version)
            .await?;
        let path_volume = crate::volume::PathVolume::new(volume);

        // interfacing with pond::Volume through a channel, following the Communicating Sequential
        // Processes (CSP) pattern. this serializes all operations to pond::Volume making it
        // thread-safe (which pond::Volume isn't).
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(cmd::dispatch_loop(path_volume, rx));

        Ok(Self { cmds: tx })
    }

    pub async fn version(&self) -> pond::Version {
        let (tx, rx) = oneshot::channel();
        let cmd = cmd::Cmd::Version { resp: tx };
        let _ = self.cmds.send(cmd).await;
        rx.await.expect("volume exited unexpectedly")
    }

    /// Commit all staged changes in a volume, persisting it into the backend for this volume.
    pub async fn commit(&mut self, version: impl ToString) -> pond::Result<()> {
        let (tx, rx) = oneshot::channel();
        let cmd = cmd::Cmd::Commit {
            version: version.to_string(),
            resp: tx,
        };
        let _ = self.cmds.send(cmd).await;
        rx.await.expect("volume exited unexpectedly")
    }

    /// Fetch metadata of a file if it exists.
    pub async fn metadata(&self, path: impl ToString) -> pond::Result<FileAttr> {
        let (tx, rx) = oneshot::channel();
        let cmd = cmd::Cmd::Metadata {
            path: path.to_string(),
            resp: tx,
        };
        let _ = self.cmds.send(cmd).await;
        rx.await.expect("volume exited unexpectedly")
    }

    /// Check if a file or directory exists.
    pub async fn exists(&self, path: impl ToString) -> pond::Result<bool> {
        let (tx, rx) = oneshot::channel();
        let cmd = cmd::Cmd::Exists {
            path: path.to_string(),
            resp: tx,
        };
        let _ = self.cmds.send(cmd).await;
        rx.await.expect("volume exited unexpectedly")
    }

    /// Iterate over the directory entries within the given directory.
    ///
    /// The return value holds a read-lock on the volume.
    pub async fn read_dir(&self, path: impl ToString) -> pond::Result<()> {
        let (tx, rx) = oneshot::channel();
        let cmd = cmd::Cmd::ReadDir {
            path: path.to_string(),
            resp: tx,
        };
        let _ = self.cmds.send(cmd).await;
        rx.await.expect("volume exited unexpectedly")
    }

    /// Create a directory in the volume.
    pub async fn create_dir(&mut self, path: impl ToString) -> pond::Result<FileAttr> {
        let (tx, rx) = oneshot::channel();
        let cmd = cmd::Cmd::CreateDir {
            path: path.to_string(),
            resp: tx,
        };
        let _ = self.cmds.send(cmd).await;
        rx.await.expect("volume exited unexpectedly")
    }

    /// Create a directory in the volume.
    pub async fn create_dir_all(&self, path: impl ToString) -> pond::Result<FileAttr> {
        let (tx, rx) = oneshot::channel();
        let cmd = cmd::Cmd::CreateDirAll {
            path: path.to_string(),
            resp: tx,
        };
        let _ = self.cmds.send(cmd).await;
        rx.await.expect("volume exited unexpectedly")
    }

    /// Remove a directory from the volume.
    pub async fn remove_dir(&mut self, path: impl ToString) -> pond::Result<()> {
        let (tx, rx) = oneshot::channel();
        let cmd = cmd::Cmd::RemoveDir {
            path: path.to_string(),
            resp: tx,
        };
        let _ = self.cmds.send(cmd).await;
        rx.await.expect("volume exited unexpectedly")
    }

    /// Remove a directory from the volume.
    pub async fn remove_dir_all(&mut self, path: impl ToString) -> pond::Result<()> {
        let (tx, rx) = oneshot::channel();
        let cmd = cmd::Cmd::RemoveDirAll {
            path: path.to_string(),
            resp: tx,
        };
        let _ = self.cmds.send(cmd).await;
        rx.await.expect("volume exited unexpectedly")
    }

    pub async fn open(&self, path: impl ToString) -> pond::Result<()> {
        let (tx, rx) = oneshot::channel();
        let cmd = cmd::Cmd::Open {
            path: path.to_string(),
            resp: tx,
        };
        let _ = self.cmds.send(cmd).await;
        rx.await.expect("volume exited unexpectedly")
    }

    pub async fn remove_file(&mut self, path: impl ToString) -> pond::Result<()> {
        let (tx, rx) = oneshot::channel();
        let cmd = cmd::Cmd::RemoveFile {
            path: path.to_string(),
            resp: tx,
        };
        let _ = self.cmds.send(cmd).await;
        rx.await.expect("volume exited unexpectedly")
    }

    pub async fn copy(&self, src: impl ToString, dst: impl ToString) -> pond::Result<()> {
        let (tx, rx) = oneshot::channel();
        let cmd = cmd::Cmd::Copy {
            src: src.to_string(),
            dst: dst.to_string(),
            resp: tx,
        };
        let _ = self.cmds.send(cmd).await;
        rx.await.expect("volume exited unexpectedly")
    }

    pub async fn rename(&self, src: impl ToString, dst: impl ToString) -> pond::Result<()> {
        let (tx, rx) = oneshot::channel();
        let cmd = cmd::Cmd::Rename {
            src: src.to_string(),
            dst: dst.to_string(),
            resp: tx,
        };
        let _ = self.cmds.send(cmd).await;
        rx.await.expect("volume exited unexpectedly")
    }

    pub async fn touch(&self, path: impl ToString) -> pond::Result<FileAttr> {
        let (tx, rx) = oneshot::channel();
        let cmd = cmd::Cmd::Touch {
            path: path.to_string(),
            resp: tx,
        };
        let _ = self.cmds.send(cmd).await;
        rx.await.expect("volume exited unexpectedly")
    }
}
