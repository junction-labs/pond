use pond::FileAttr;
use tokio::sync::{mpsc, oneshot};

use crate::volume::PathVolume;

#[derive(Debug)]
pub(crate) enum Cmd {
    Version {
        resp: oneshot::Sender<pond::Version>,
    },
    Commit {
        version: String,
        resp: oneshot::Sender<Result<(), pond::Error>>,
    },
    Metadata {
        path: String,
        resp: oneshot::Sender<pond::Result<FileAttr>>,
    },
    Exists {
        path: String,
        resp: oneshot::Sender<pond::Result<bool>>,
    },
    ReadDir {
        path: String,
        resp: oneshot::Sender<pond::Result<()>>,
    },
    CreateDir {
        path: String,
        resp: oneshot::Sender<pond::Result<FileAttr>>,
    },
    CreateDirAll {
        path: String,
        resp: oneshot::Sender<pond::Result<FileAttr>>,
    },
    RemoveDir {
        path: String,
        resp: oneshot::Sender<pond::Result<()>>,
    },
    RemoveDirAll {
        path: String,
        resp: oneshot::Sender<pond::Result<()>>,
    },
    Open {
        path: String,
        resp: oneshot::Sender<pond::Result<()>>,
    },
    RemoveFile {
        path: String,
        resp: oneshot::Sender<pond::Result<()>>,
    },
    Copy {
        src: String,
        dst: String,
        resp: oneshot::Sender<pond::Result<()>>,
    },
    Rename {
        src: String,
        dst: String,
        resp: oneshot::Sender<pond::Result<()>>,
    },
    Touch {
        path: String,
        resp: oneshot::Sender<pond::Result<FileAttr>>,
    },
}

/// Dispatches the Cmd to the corresponding Volume method.
async fn dispatch(volume: &mut PathVolume, cmd: Cmd) {
    match cmd {
        Cmd::Version { resp } => {
            let _ = resp.send(volume.version());
        }
        Cmd::Commit { version, resp } => {
            let _ = resp.send(volume.commit(version).await);
        }
        Cmd::Metadata { path, resp } => {
            let _ = resp.send(volume.metadata(path).await);
        }
        Cmd::Exists { path, resp } => {
            let _ = resp.send(volume.exists(path).await);
        }
        Cmd::ReadDir { path, resp } => {
            let _ = resp.send(volume.read_dir(path).await);
        }
        Cmd::CreateDir { path, resp } => {
            let _ = resp.send(volume.create_dir(path).await);
        }
        Cmd::CreateDirAll { path, resp } => {
            let _ = resp.send(volume.create_dir_all(path).await);
        }
        Cmd::RemoveDir { path, resp } => {
            let _ = resp.send(volume.remove_dir(path).await);
        }
        Cmd::RemoveDirAll { path, resp } => {
            let _ = resp.send(volume.remove_dir_all(path).await);
        }
        Cmd::Open { path, resp } => {
            let _ = resp.send(volume.open(path).await);
        }
        Cmd::RemoveFile { path, resp } => {
            let _ = resp.send(volume.remove_file(path).await);
        }
        Cmd::Copy { src, dst, resp } => {
            let _ = resp.send(volume.copy(src, dst).await);
        }
        Cmd::Rename { src, dst, resp } => {
            let _ = resp.send(volume.rename(src, dst).await);
        }
        Cmd::Touch { path, resp } => {
            let _ = resp.send(volume.touch(path).await);
        }
    }
}

/// Process and dispatch Cmds on the receiving end of the mpsc::channel.
pub(crate) async fn dispatch_loop(mut volume: PathVolume, mut cmds: mpsc::Receiver<Cmd>) {
    while let Some(cmd) = cmds.recv().await {
        dispatch(&mut volume, cmd).await;
    }
}
