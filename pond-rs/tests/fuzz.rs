#![allow(clippy::print_stderr)]

use std::{
    path::{Path, PathBuf},
    time::SystemTime,
};

use arbitrary::{Arbitrary, Unstructured};
use arbtest::arbtest;
use pond::{Client, Error, ErrorKind, FileAttr, FileType, Ino, Version};
use pond_rs::{CacheConfig, Volume};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[test]
fn fuzz_volume_ops() {
    let fuzz_dir = project_root().join("target/pond/integration/pond_rs_fuzz");
    let volume_dir = fuzz_dir.join("volume");
    let reference_dir = fuzz_dir.join("reference");
    std::fs::create_dir_all(&fuzz_dir).unwrap();

    arbtest(|u| {
        test_volume_ops(&volume_dir, &reference_dir, arbitrary_vec(u)?);
        Ok(())
    });
}

fn test_volume_ops(volume_dir: &Path, reference_dir: &Path, ops: Vec<FuzzOp>) {
    reset_dir(volume_dir);
    reset_dir(reference_dir);

    let runtime = test_runtime();
    let volume = runtime.block_on(async {
        let mut client = Client::new(volume_dir.to_str().unwrap()).unwrap();
        let mut base = client.create_volume().await;
        base.commit(Version::from_static("init")).await.unwrap();
        Volume::load(volume_dir.to_str().unwrap(), None, CacheConfig::default())
            .await
            .unwrap()
    });

    let reference_res: Vec<_> = ops.iter().map(|op| apply_fs(reference_dir, op)).collect();
    let actual_res: Vec<_> = ops
        .iter()
        .map(|op| runtime.block_on(apply_volume(&volume, op)))
        .collect();

    if reference_res != actual_res {
        eprintln!("operation history:");
        for (idx, op) in ops.iter().enumerate() {
            let expected = &reference_res[idx];
            let actual = &actual_res[idx];
            if expected != actual {
                eprintln!("{op} --> expected={expected:?} actual={actual:?} <--");
            } else {
                eprintln!("{op}");
            }
        }
        assert_eq!(reference_res, actual_res, "results differed");
    }
}

async fn apply_volume(volume: &Volume, op: &FuzzOp) -> Result<OpOutput, std::io::ErrorKind> {
    match op {
        FuzzOp::CreateDir(path) => volume
            .create_dir(path.for_volume())
            .await
            .map(OpOutput::Metadata)
            .map_err(pond_err),
        FuzzOp::RemoveDir(path) => volume
            .remove_dir(path.for_volume())
            .await
            .map(|_| OpOutput::None)
            .map_err(pond_err),
        FuzzOp::RemoveFile(path) => volume
            .remove_file(path.for_volume())
            .await
            .map(|_| OpOutput::None)
            .map_err(pond_err),
        FuzzOp::Write(path, data) => {
            match volume.remove_file(path.for_volume()).await {
                Ok(_) => {}
                Err(e) if matches!(e.kind(), ErrorKind::NotFound) => {}
                Err(e) if matches!(e.kind(), ErrorKind::IsADirectory) => {
                    return Err(std::io::ErrorKind::IsADirectory);
                }
                Err(e) => return Err(pond_err(e)),
            }
            volume.touch(path.for_volume()).await.map_err(pond_err)?;
            let mut file = volume
                .open_read_write(path.for_volume())
                .await
                .map_err(pond_err)?;
            file.write_all(data.as_bytes())
                .await
                .map_err(|e| e.kind())?;
            Ok(OpOutput::Write(data.len()))
        }
        FuzzOp::Read(path) => {
            let meta = volume.metadata(path.for_volume()).await.map_err(pond_err)?;
            if matches!(meta.kind, FileType::Directory) {
                return Err(std::io::ErrorKind::IsADirectory);
            }
            let mut file = volume
                .open_read(path.for_volume())
                .await
                .map_err(pond_err)?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).await.map_err(|e| e.kind())?;
            String::from_utf8(buf)
                .map(OpOutput::Read)
                .map_err(|_| std::io::ErrorKind::InvalidData)
        }
        FuzzOp::Metadata(path) => volume
            .metadata(path.for_volume())
            .await
            .map(OpOutput::Metadata)
            .map_err(pond_err),
    }
}

fn apply_fs(root: &Path, op: &FuzzOp) -> Result<OpOutput, std::io::ErrorKind> {
    match op {
        FuzzOp::CreateDir(path) => {
            let path = root.join(path);
            std::fs::create_dir(&path).map_err(|e| e.kind())?;
            fileattr(&path)
                .map(OpOutput::Metadata)
                .map_err(|e| e.kind())
        }
        FuzzOp::RemoveDir(path) => std::fs::remove_dir(root.join(path))
            .map(|_| OpOutput::None)
            .map_err(|e| e.kind()),
        FuzzOp::RemoveFile(path) => {
            let path = root.join(path);
            if path.is_dir() {
                return Err(std::io::ErrorKind::IsADirectory);
            }
            std::fs::remove_file(path)
                .map(|_| OpOutput::None)
                .map_err(|e| e.kind())
        }
        FuzzOp::Write(path, data) => {
            let path = root.join(path);
            match std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&path)
            {
                Ok(mut file) => std::io::Write::write_all(&mut file, data.as_bytes())
                    .map(|_| OpOutput::Write(data.len()))
                    .map_err(|e| e.kind()),
                Err(e) => Err(e.kind()),
            }
        }
        FuzzOp::Read(path) => std::fs::read_to_string(root.join(path))
            .map(OpOutput::Read)
            .map_err(|e| e.kind()),
        FuzzOp::Metadata(path) => fileattr(&root.join(path))
            .map(OpOutput::Metadata)
            .map_err(|e| e.kind()),
    }
}

fn fileattr(path: &Path) -> std::io::Result<FileAttr> {
    std::fs::metadata(path).map(|meta| {
        let kind = if meta.is_file() {
            FileType::Regular
        } else {
            FileType::Directory
        };
        FileAttr {
            ino: Ino::None,
            size: meta.len(),
            mtime: SystemTime::UNIX_EPOCH,
            ctime: SystemTime::UNIX_EPOCH,
            kind,
        }
    })
}

fn pond_err(err: Error) -> std::io::ErrorKind {
    match err.kind() {
        ErrorKind::NotFound => std::io::ErrorKind::NotFound,
        ErrorKind::AlreadyExists => std::io::ErrorKind::AlreadyExists,
        ErrorKind::NotADirectory => std::io::ErrorKind::NotADirectory,
        ErrorKind::DirectoryNotEmpty => std::io::ErrorKind::DirectoryNotEmpty,
        ErrorKind::PermissionDenied => std::io::ErrorKind::PermissionDenied,
        ErrorKind::IsADirectory => std::io::ErrorKind::IsADirectory,
        ErrorKind::Unsupported => std::io::ErrorKind::Unsupported,
        ErrorKind::InvalidData => std::io::ErrorKind::InvalidData,
        _ => std::io::ErrorKind::Other,
    }
}

fn arbitrary_vec<'a, T: Arbitrary<'a>>(u: &mut Unstructured<'a>) -> arbitrary::Result<Vec<T>> {
    let len = u.arbitrary_len::<usize>()?;
    let mut items = Vec::with_capacity(len);
    for _ in 0..len {
        items.push(u.arbitrary()?);
    }
    Ok(items)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ArbPath(PathBuf);

impl ArbPath {
    fn for_volume(&self) -> String {
        format!("/{}", self.0.display())
    }
}

impl std::fmt::Display for ArbPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.display())
    }
}

impl AsRef<Path> for ArbPath {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl<'a> Arbitrary<'a> for ArbPath {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        const SEGMENTS: &[&str] = &["aaa", "bbb", "ccc"];
        let depth = u.int_in_range(1..=3)?;
        let mut path = PathBuf::new();
        for _ in 0..depth {
            path.push(u.choose(SEGMENTS)?);
        }
        Ok(Self(path))
    }
}

#[derive(Debug, Clone, Arbitrary)]
enum FuzzOp {
    CreateDir(ArbPath),
    RemoveDir(ArbPath),
    RemoveFile(ArbPath),
    Write(ArbPath, String),
    Read(ArbPath),
    Metadata(ArbPath),
}

impl std::fmt::Display for FuzzOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FuzzOp::CreateDir(path) => write!(f, "mkdir {path}"),
            FuzzOp::RemoveDir(path) => write!(f, "rmdir {path}"),
            FuzzOp::RemoveFile(path) => write!(f, "rmfile {path}"),
            FuzzOp::Write(path, data) => write!(f, "write {path} ({})", data.len()),
            FuzzOp::Read(path) => write!(f, "read {path}"),
            FuzzOp::Metadata(path) => write!(f, "metadata {path}"),
        }
    }
}

#[derive(Debug, Clone)]
enum OpOutput {
    None,
    Read(String),
    Metadata(FileAttr),
    Write(usize),
}

impl PartialEq for OpOutput {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::None, Self::None) => true,
            (Self::Read(a), Self::Read(b)) => a == b,
            (Self::Metadata(a), Self::Metadata(b)) => {
                if a.kind == FileType::Regular {
                    a.size == b.size && a.kind == b.kind
                } else {
                    // pond doesn't set a size for directories, so ignore it
                    a.kind == b.kind
                }
            }
            (Self::Write(a), Self::Write(b)) => a == b,
            _ => false,
        }
    }
}

/// No-op, PartialEq is all we need.
impl Eq for OpOutput {}

fn reset_dir(path: &Path) {
    let _ = std::fs::remove_dir_all(path);
    std::fs::create_dir_all(path).unwrap();
}

fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(1)
        .unwrap()
        .to_path_buf()
}

fn test_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
