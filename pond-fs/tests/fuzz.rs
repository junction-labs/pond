// print-in-tests from clippy config doesn't seem to cover integration tests
// so we're explicitly allowing it here.
#![allow(clippy::print_stderr)]

use std::{
    collections::BTreeMap,
    io::ErrorKind,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arbitrary::{Arbitrary, Unstructured};
use arbtest::arbtest;
use pond::{Client, Version, Volume};

// TODO: try cargo-fuzz. arbtest is great and simple, but doesn't help us save
// known-bad seeds or anything like that.

/// Mount an empty volume and fuzz it by generating random file operations
/// comparing them to the local filesystem.
#[test]
fn fuzz_empty_volume() {
    let fuzz_dir = project_root().join("target/pond/integration/fuzz_empty");
    std::fs::create_dir_all(&fuzz_dir).unwrap();

    let expected_dir = fuzz_dir.join("expected");
    let actual_dir = fuzz_dir.join("actual");

    arbtest(|u| {
        test_empty_volume(&expected_dir, &actual_dir, arbitrary_vec(u)?);
        Ok(())
    });
}

fn test_empty_volume(expected_dir: &Path, actual_dir: &Path, ops: Vec<FuzzOp>) {
    // for every run, remove everything in the test dirs and recreate them
    // as empty.
    reset_dir(expected_dir);
    reset_dir(actual_dir);

    let mut client = Client::create("memory://").unwrap();
    let volume = test_runtime().block_on(client.create_volume());
    let _mount = spawn_mount(actual_dir, volume);

    // apply but ignore errors when trying to commit with the same
    // version twice. they should be no-ops - we know they're different
    // in the reference fs and we don't care.
    let apply = |dir: &Path, op: &FuzzOp, assert_mtime: bool| {
        let res = apply_op(dir, op, assert_mtime);
        match (op, res) {
            (FuzzOp::Commit(_), Err(e)) if e.kind() == ErrorKind::AlreadyExists => {
                Ok(OpOutput::None)
            }
            (_, res) => res.map_err(|e| e.kind()),
        }
    };
    // sometimes the kernel cache holds onto the mtime for too long, so let's skip checking it for
    // the reference filesystem
    let reference_res: Vec<_> = ops
        .iter()
        .map(|op| apply(expected_dir, op, false))
        .collect();
    let test_res: Vec<_> = ops.iter().map(|op| apply(actual_dir, op, true)).collect();

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
}

#[test]
fn fuzz_pack() {
    let fuzz_dir = project_root().join("target/pond/integration/fuzz_pack");
    let pack_dir = fuzz_dir.join("volume");
    let expected_dir = fuzz_dir.join("expected");
    let actual_dir = fuzz_dir.join("actual");
    std::fs::create_dir_all(&fuzz_dir).unwrap();

    // pack a file with empty data
    test_pack(
        &expected_dir,
        &actual_dir,
        &pack_dir,
        vec![
            FuzzEntry::file("foo", "123"),
            FuzzEntry::file("bar", "456"),
            FuzzEntry::file("baz", ""),
        ],
    );

    arbtest(|u| {
        test_pack(&expected_dir, &actual_dir, &pack_dir, arbitrary_vec(u)?);
        Ok(())
    });
}

fn test_pack(expected_dir: &Path, actual_dir: &Path, pack_dir: &Path, entries: Vec<FuzzEntry>) {
    reset_dir(expected_dir);
    reset_dir(actual_dir);
    reset_dir(pack_dir);

    let runtime = test_runtime();
    let version = Version::from_static("123");
    let mut client = Client::create(pack_dir.to_str().unwrap()).unwrap();

    // create a random test volume
    let mkdir = |p: &Path| create_dir_all(p, [ErrorKind::AlreadyExists, ErrorKind::NotADirectory]);

    for entry in entries {
        match entry {
            FuzzEntry::Dir(path) => {
                let path = expected_dir.join(path);
                mkdir(path.as_ref()).unwrap()
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
    runtime
        .block_on(async {
            let volume = client.create_volume().await;
            volume.pack(expected_dir, version).await?;
            Ok::<_, pond::Error>(())
        })
        .unwrap();

    // mount the new dir as filesystem
    let volume = runtime.block_on(client.load_volume(&None)).unwrap();
    let _mount = spawn_mount(actual_dir, volume);

    // walk both directories and see if we have the same files and directories
    let expected = read_entries(expected_dir);
    let actual = read_entries(actual_dir);

    assert_eq!(expected, actual, "pack entries differ");
}

#[test]
fn fuzz_commit() {
    let fuzz_dir = project_root().join("target/pond/integration/fuzz_commit");
    let expected_dir = fuzz_dir.join("expected");
    let mount_dir = fuzz_dir.join("mount");
    let volume_dir = fuzz_dir.join("volume");
    std::fs::create_dir_all(&fuzz_dir).unwrap();

    // commit after truncating a file
    test_commit(
        &expected_dir,
        &mount_dir,
        &volume_dir,
        vec![
            FuzzEntry::file("aaa", "123"),
            FuzzEntry::file("aaa", ""),
            FuzzEntry::file("bbb", "hi"),
        ],
        vec![],
    );

    arbtest(|u| {
        test_commit(
            &expected_dir,
            &mount_dir,
            &volume_dir,
            arbitrary_vec(u)?,
            arbitrary_vec(u)?,
        );
        Ok(())
    });
}

fn test_commit(
    expected_dir: &Path,
    mount_dir: &Path,
    volume_dir: &Path,
    pre_commit_entries: Vec<FuzzEntry>,
    lost_entries: Vec<FuzzEntry>,
) {
    // for every run, remove everything in the test dirs and recreate them
    // as empty.
    reset_dir(expected_dir);
    reset_dir(mount_dir);
    reset_dir(volume_dir);

    let runtime = test_runtime();
    let mut client = Client::create(volume_dir.to_str().unwrap()).unwrap();

    // create an empty volume and write a bunch of files and directories to it.
    // write the same entries to a local filesystem as a reference.
    let volume = runtime.block_on(client.create_volume());

    let mount = spawn_mount(mount_dir, volume);
    for entry in &pre_commit_entries {
        apply_entry(expected_dir, entry).unwrap();
        apply_entry(mount_dir, entry).unwrap();
    }

    // check the ctimes of the files before we commit, they should be updated after the commit is
    // done
    let before_commit = read_entries(mount_dir);
    let before_commit_ctimes: Vec<_> = before_commit
        .iter()
        .filter_map(|e| match e {
            FuzzEntry::File(arb_path, _) => ctime(&mount_dir.join(arb_path)).unwrap(),
            _ => None,
        })
        .collect();

    // commit
    apply_op(mount_dir, &FuzzOp::Commit("v1".to_string()), true).unwrap();

    // compare the ctimes after the commit happens
    {
        let mut after_commit_ctimes: Vec<_> = read_entries(mount_dir)
            .into_iter()
            .filter_map(|e| match e {
                FuzzEntry::File(arb_path, _) => ctime(&mount_dir.join(arb_path)).unwrap(),
                _ => None,
            })
            .collect();
        after_commit_ctimes.sort();
        after_commit_ctimes.dedup();

        if before_commit
            .iter()
            .any(|e| matches!(e, FuzzEntry::File(..)))
        {
            assert_eq!(after_commit_ctimes.len(), 1);
        } else {
            assert_eq!(after_commit_ctimes.len(), 0);
        }

        if let Some(ctime) = after_commit_ctimes.first() {
            assert!(before_commit_ctimes.iter().all(|e| e <= ctime));
        }
    }

    // write a whole bunch of stuff to the mount, then drop it without
    // committing. we should lose all of this.
    for entry in &lost_entries {
        apply_entry(mount_dir, entry).unwrap();
    }
    std::mem::drop(mount);

    // reload the volume at the lastest version and assert that
    // we have all the data from before the commit and nothing
    // from after it.
    let volume = runtime.block_on(client.load_volume(&None)).unwrap();
    assert_eq!(volume.version(), Version::from_static("v1"));

    let mount = spawn_mount(mount_dir, volume);
    let expected = read_entries(expected_dir);
    let actual = read_entries(mount_dir);

    if expected != actual {
        let expected = to_entry_map(expected);
        let actual = to_entry_map(actual);
        let before_commit = to_entry_map(before_commit);
        let lost_entries = to_entry_map(lost_entries);

        for entry in pre_commit_entries {
            let path = entry.path();

            let actual = actual.get(path);
            let expected = expected.get(path);
            let lost = lost_entries.get(path);
            let before_commit = before_commit.get(path);

            eprint!("{entry}");
            if actual != expected {
                eprint!(
                    " --> actual={actual:?} expected={expected:?} before_commit={before_commit:?} lost={lost:?} <--"
                );
            }
            eprintln!()
        }
        assert_eq!(expected, actual, "entries differ");
    }

    std::mem::drop(mount);
}

fn to_entry_map(v: Vec<FuzzEntry>) -> BTreeMap<PathBuf, FuzzEntry> {
    v.into_iter().map(|e| (e.path().to_path_buf(), e)).collect()
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
        if let Some(s) = self.0.take() {
            s.join()
        }
    }
}

// spawn a new mount on a background thread
fn spawn_mount(mountpoint: impl AsRef<Path>, volume: Volume) -> AutoUnmount {
    let session = pond_fs::mount_volume(
        test_runtime(),
        volume,
        mountpoint,
        // we explicitly do not want to set auto_unmount here - it spawns a
        // fusermount3 child process that keeps the fuse mount alive as long
        // as this PROCESS is alive. that means we can't unmount/remount
        // in different tests.
        false,          // allow_other
        false,          // auto_unmount
        Duration::ZERO, // kernel_cache_timeout
    )
    .unwrap();

    // start in the background and let it rip
    session.spawn().unwrap().into()
}

fn arbitrary_vec<'a, T: Arbitrary<'a>>(
    u: &mut Unstructured<'a>,
) -> Result<Vec<T>, arbitrary::Error> {
    let size = u.arbitrary_len::<usize>()?;
    let mut rv = Vec::with_capacity(size);
    for _ in 0..size {
        rv.push(u.arbitrary()?);
    }
    Ok(rv)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ArbPath(PathBuf);

impl AsRef<Path> for ArbPath {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl<T: Into<PathBuf>> From<T> for ArbPath {
    fn from(value: T) -> Self {
        Self(value.into())
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, PartialOrd, Ord)]
enum FuzzEntry {
    Dir(ArbPath),
    File(ArbPath, String),
}

impl std::fmt::Display for FuzzEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FuzzEntry::Dir(path) => write!(f, "mkdir {path}"),
            FuzzEntry::File(path, data) => write!(f, "write  {path}, {data:#?}"),
        }
    }
}

impl FuzzEntry {
    fn file(path: impl AsRef<str>, content: impl AsRef<str>) -> Self {
        let path = path.as_ref().into();
        let content = content.as_ref().to_string();
        Self::File(path, content)
    }

    fn path(&self) -> &Path {
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
        if path.as_os_str().is_empty() {
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

    entries.sort_by(|a, b| a.path().cmp(b.path()));
    entries
}

fn apply_entry(root: impl AsRef<Path>, entry: &FuzzEntry) -> std::io::Result<()> {
    let root = root.as_ref();
    match entry {
        FuzzEntry::Dir(path) => {
            let path = root.join(path);
            create_dir_all(path, [ErrorKind::AlreadyExists, ErrorKind::NotADirectory])
        }
        FuzzEntry::File(path, content) => {
            let path = root.join(path);
            let parent = path
                .parent()
                .expect("BUG: generated a fuzz entry with no parent");

            // allow errors this time, just bail when we see them
            if create_dir_all(parent, []).is_err() {
                return Ok(());
            }
            match std::fs::write(path, content) {
                Ok(_) => Ok(()),
                Err(e) if e.kind() == ErrorKind::IsADirectory => Ok(()),
                Err(e) => Err(e),
            }
        }
    }
}

#[derive(Debug, Clone, Arbitrary)]
enum FuzzOp {
    Mkdir(ArbPath),
    RmDir(ArbPath),
    Rename(ArbPath, ArbPath),
    Read(ArbPath),
    Write(ArbPath, String),
    Remove(ArbPath),
    Commit(String),
}

impl std::fmt::Display for FuzzOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FuzzOp::Mkdir(path) => write!(f, "mkdir  {path}"),
            FuzzOp::RmDir(path) => write!(f, "rmdir  {path}"),
            FuzzOp::Rename(from, to) => write!(f, "rename {from} -> {to}"),
            FuzzOp::Read(path) => write!(f, "read   {path}"),
            FuzzOp::Write(path, data) => write!(f, "write  {path}, {data:#?}"),
            FuzzOp::Remove(path) => write!(f, "remove {path}"),
            FuzzOp::Commit(version) => write!(f, "commit version={version:#?}"),
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

fn mtime(path: &PathBuf) -> Result<Option<SystemTime>, std::io::Error> {
    if !std::fs::exists(path)? {
        return Ok(None);
    }

    std::fs::metadata(path)?.modified().map(Some)
}

fn ctime(path: &PathBuf) -> Result<Option<SystemTime>, std::io::Error> {
    if !std::fs::exists(path)? {
        return Ok(None);
    }

    let md = std::fs::metadata(path)?;
    let systime = UNIX_EPOCH + Duration::new(md.ctime() as u64, md.ctime_nsec() as u32);
    Ok(Some(systime))
}

fn apply_op(
    root: impl AsRef<Path>,
    op: &FuzzOp,
    assert_mtime: bool,
) -> Result<OpOutput, std::io::Error> {
    let root = root.as_ref();

    match op {
        FuzzOp::Mkdir(path) => {
            let path = root.join(path);
            tri!(std::fs::create_dir_all(path));
            Ok(OpOutput::None)
        }
        FuzzOp::RmDir(path) => {
            let path = root.join(path);
            tri!(std::fs::remove_dir(path));
            Ok(OpOutput::None)
        }
        FuzzOp::Rename(from, to) => {
            let from = root.join(from);
            let to = root.join(to);
            tri!(std::fs::rename(from, to));
            Ok(OpOutput::None)
        }
        FuzzOp::Read(path) => {
            let path = root.join(path);
            let bs = tri!(std::fs::read_to_string(path));
            Ok(OpOutput::Read(bs))
        }
        FuzzOp::Write(path, data) => {
            let path = root.join(path);
            let prev_mtime = mtime(&path)?;
            tri!(std::fs::write(&path, data));
            if assert_mtime {
                // (1) file didn't exist before, so prev_mtime is None
                // (2) previous mtime was bumped after we wrote
                // (3) the two writes happened in the same ns, so they're equal. make sure they're
                //     equal to now
                let mtime = mtime(&path)?;
                assert!(
                    prev_mtime.is_none() || prev_mtime < mtime,
                    "{:?} < {:?}",
                    prev_mtime,
                    mtime,
                );
            }
            Ok(OpOutput::Write(data.len()))
        }
        FuzzOp::Remove(path) => {
            let path = root.join(path);
            tri!(std::fs::remove_file(path));
            Ok(OpOutput::None)
        }
        FuzzOp::Commit(version) => {
            let path = root.join(".commit");
            if path.exists() {
                commit_with_retry(&path, version)?;
            }
            Ok(OpOutput::None)
        }
    }
}

fn commit_with_retry(path: &Path, version: &str) -> std::io::Result<()> {
    const ATTEMPTS: usize = 3;

    for attempt in 1..=ATTEMPTS {
        match std::fs::write(path, version) {
            Ok(()) => return Ok(()),
            Err(err) if err.kind() == ErrorKind::ResourceBusy && attempt < ATTEMPTS => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(err) => return Err(err),
        }
    }

    Ok(())
}
