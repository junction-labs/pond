use std::{
    io::ErrorKind,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use arbitrary::{Arbitrary, Unstructured};
use arbtest::arbtest;
use cfs_core::{Location, Volume, VolumeMetadata};
use cfs_fuse::pack;
use nix::NixPath;
use object_store::ObjectStore;

// TODO: try cargo-fuzz. arbtest is great and simple, but doesn't help us save
// known-bad seeds or anything like that.

/// Mount an empty volume and fuzz it by generating random file operations
/// comparing them to the local filesystem.
#[test]
fn fuzz_empty_volume() {
    let fuzz_dir = project_root().join("target/cfs/integration/fuzz_empty");
    let expected_dir = fuzz_dir.join("expected");
    let actual_dir = fuzz_dir.join("actual");

    // make sure the fuzz dir exists
    std::fs::create_dir_all(&fuzz_dir).unwrap();
    arbtest(|u| {
        // for every run, remove everything in the test dirs and recreate them
        // as empty.
        reset_dir(&expected_dir);
        reset_dir(&actual_dir);

        let ops: Vec<FuzzOp> = u.arbitrary()?;

        let reference_res: Vec<_> = ops.iter().map(|op| apply(&expected_dir, op)).collect();

        let _mount = spawn_mount(&actual_dir, VolumeMetadata::empty(), None);
        let test_res: Vec<_> = ops.iter().map(|op| apply(&actual_dir, op)).collect();

        // print our own error history so it's easy to spot when things don't match.
        if reference_res != test_res {
            eprintln!("op history:");
            for (i, op) in ops.iter().enumerate() {
                let expected = &reference_res[i];
                let actual = &test_res[i];
                if expected != actual {
                    eprintln!("{op}  --> expected={expected:?} != actual={actual:?} <--");
                } else {
                    eprintln!("{op}");
                }
            }
            assert_eq!(reference_res, test_res, "results differ");
        }

        Ok(())
    });
}

#[test]
fn fuzz_pack() {
    let fuzz_dir = project_root().join("target/cfs/integration/fuzz_pack");
    let pack_dir = fuzz_dir.join("volume");
    let expected_dir = fuzz_dir.join("expected");
    let actual_dir = fuzz_dir.join("actual");
    std::fs::create_dir_all(&fuzz_dir).unwrap();

    arbtest(|u| {
        reset_dir(&expected_dir);
        reset_dir(&actual_dir);
        reset_dir(&pack_dir);

        // create a random test volume
        let mut entries: Vec<FuzzEntry> = u.arbitrary()?;
        if entries.is_empty() {
            entries.push(u.arbitrary()?);
        }

        let mkdir =
            |p: &Path| create_dir_all(p, [ErrorKind::AlreadyExists, ErrorKind::NotADirectory]);

        for entry in entries {
            match entry {
                FuzzEntry::Dir(path) => {
                    let path = expected_dir.join(path);
                    mkdir(&path.as_ref()).unwrap()
                }
                FuzzEntry::File(path, content) => {
                    let path = expected_dir.join(path);
                    let parent = path.parent().unwrap();
                    if mkdir(parent).is_err() {
                        continue;
                    }
                    let _ = std::fs::write(path, content);
                }
            }
        }

        // pack it to the pack_dir
        pack(
            test_runtime(),
            &expected_dir,
            pack_dir.display().to_string(),
            None,
        )
        .unwrap();

        // mount the new dir as filesystem
        let volume_file = find_volume(&pack_dir).unwrap();
        let metadata = VolumeMetadata::from_bytes(&std::fs::read(&volume_file).unwrap()).unwrap();

        let _mount = spawn_mount(&actual_dir, metadata, Some(&actual_dir));

        // walk both directories and see if we have the same files and directories
        let expected = read_entries(&expected_dir);
        dbg!(&expected);
        let actual = read_entries(&actual_dir);

        if expected != actual {
            assert_eq!(expected, actual, "pack entries differ");
        }

        Ok(())
    });
}

fn reset_dir(p: impl AsRef<Path>) {
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap()
}

fn create_dir_all(
    p: impl AsRef<Path>,
    allow: impl IntoIterator<Item = std::io::ErrorKind>,
) -> std::io::Result<()> {
    match std::fs::create_dir_all(p) {
        Ok(()) => Ok(()),
        Err(e) if allow.into_iter().any(|k| k == e.kind()) => Ok(()),
        Err(e) => Err(e),
    }
}

fn find_volume(dir: impl AsRef<Path>) -> std::io::Result<PathBuf> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map(|s| s.as_bytes()) == Some(b"volume") {
            return Ok(path);
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "no volume found",
    ))
}

fn project_root() -> PathBuf {
    Path::new(&env!("CARGO_MANIFEST_DIR"))
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

struct AutoUnmount(Option<fuser::BackgroundSession>);

impl From<fuser::BackgroundSession> for AutoUnmount {
    fn from(value: fuser::BackgroundSession) -> Self {
        Self(Some(value))
    }
}

impl Drop for AutoUnmount {
    fn drop(&mut self) {
        self.0.take().map(|s| s.join());
    }
}

// spawn a new mount on a background thread
fn spawn_mount(
    mountpoint: impl AsRef<Path>,
    metadata: VolumeMetadata,
    local_path: Option<&Path>,
) -> AutoUnmount {
    let (location, object_store): (_, Box<dyn ObjectStore>) = match local_path {
        Some(path) => {
            let location = Location::Local {
                path: path.to_path_buf(),
            };
            let object_store = Box::new(object_store::local::LocalFileSystem::new());

            (location, object_store)
        }
        None => {
            // TODO: this is wrong, but it's to get testing right now
            let location = Location::Local {
                path: mountpoint.as_ref().to_path_buf(),
            };
            let object_store = Box::new(object_store::memory::InMemory::new());

            (location, object_store)
        }
    };

    let volume = Volume::new(
        location,
        metadata,
        1024 * 1024,
        1024,
        0,
        Arc::from(object_store),
    );

    cfs_fuse::mount_volume(
        mountpoint,
        volume,
        test_runtime(),
        // we explicitly do not want to set auto_unmount here - it spawns a
        // fusermount3 child process that keeps the fuse mount alive as long
        // as this PROCESS is alive. that means we can't unmount/remount
        // in different tests.
        false, // allow_other
        false, // auto_unmount
        false, // debug
    )
    .unwrap()
    .into()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ArbPath(PathBuf);

impl AsRef<Path> for ArbPath {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl ArbPath {
    const DEPTH: usize = 3;
    const ALPHABET: &[&str] = &["aaa", "bbb", "ccc"];
}

impl std::fmt::Display for ArbPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.display())
    }
}

impl<'a> Arbitrary<'a> for ArbPath {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let depth = u.int_in_range(1..=Self::DEPTH)?;

        let mut path = PathBuf::new();
        for _ in 0..depth {
            path.push(u.choose(Self::ALPHABET)?);
        }
        Ok(Self(path))
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
enum FuzzEntry {
    Dir(ArbPath),
    File(ArbPath, String),
}

impl FuzzEntry {
    fn dir(&self) -> &Path {
        match self {
            FuzzEntry::Dir(path) => path.as_ref(),
            FuzzEntry::File(path, _) => path.as_ref(),
        }
    }
}

fn read_entries(root: impl AsRef<Path>) -> Vec<FuzzEntry> {
    let is_hidden = |e: &walkdir::DirEntry| {
        e.file_name()
            .to_str()
            .map(|s| s.starts_with("."))
            .unwrap_or(false)
    };

    let walk = walkdir::WalkDir::new(&root)
        .into_iter()
        .filter_entry(|e| !is_hidden(e));

    let mut entries = vec![];
    for entry in walk {
        let entry = entry.unwrap();

        let path = entry.path().strip_prefix(&root).unwrap();
        if path.is_empty() {
            continue;
        }

        if entry.file_type().is_dir() {
            entries.push(FuzzEntry::Dir(ArbPath(path.to_path_buf())));
        }

        if entry.file_type().is_file() {
            let content = std::fs::read_to_string(entry.path()).unwrap();
            entries.push(FuzzEntry::File(ArbPath(path.to_path_buf()), content));
        }
    }

    entries.sort_by(|a, b| a.dir().cmp(b.dir()));
    entries
}

#[derive(Debug, Clone, Arbitrary)]
enum FuzzOp {
    Mkdir(ArbPath),
    RmDir(ArbPath),
    Read(ArbPath),
    Write(ArbPath, String),
    Remove(ArbPath),
    Commit,
}

impl std::fmt::Display for FuzzOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FuzzOp::Mkdir(path) => write!(f, "mkdir  {path}"),
            FuzzOp::RmDir(path) => write!(f, "rmdir  {path}"),
            FuzzOp::Read(path) => write!(f, "read   {path}"),
            FuzzOp::Write(path, data) => write!(f, "write  {path}, {data:#?}"),
            FuzzOp::Remove(path) => write!(f, "remove {path}"),
            FuzzOp::Commit => write!(f, "commit"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OpOutput {
    None,
    Read(String),
    Write(usize),
}

macro_rules! tri {
    ($e:expr) => {
        $e.map_err(|e| e.kind())?
    };
}

fn apply(root: impl AsRef<Path>, op: &FuzzOp) -> Result<OpOutput, std::io::ErrorKind> {
    let root = root.as_ref();

    match op {
        FuzzOp::Mkdir(path) => {
            let path = root.join(&path);
            tri!(std::fs::create_dir_all(path));
            Ok(OpOutput::None)
        }
        FuzzOp::RmDir(path) => {
            let path = root.join(&path);
            tri!(std::fs::remove_dir(path));
            Ok(OpOutput::None)
        }
        FuzzOp::Read(path) => {
            let path = root.join(&path);
            let bs = tri!(std::fs::read_to_string(path));
            Ok(OpOutput::Read(bs))
        }
        FuzzOp::Write(path, data) => {
            let path = root.join(&path);
            tri!(std::fs::write(path, &data));
            Ok(OpOutput::Write(data.len()))
        }
        FuzzOp::Remove(path) => {
            let path = root.join(&path);
            tri!(std::fs::remove_file(path));
            Ok(OpOutput::None)
        }
        FuzzOp::Commit => {
            let path = root.join(".commit");
            tri!(std::fs::write(path, "1"));
            Ok(OpOutput::None)
        }
    }
}
