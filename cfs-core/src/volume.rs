use std::{
    borrow::{Borrow, Cow},
    collections::{BTreeMap, btree_map},
    hash::{DefaultHasher, Hash, Hasher},
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

// TODO: we duplicate file/dir names as strings in data values and entry keys.
// have to figure out how to intern somewhere if we want to stop, and probably
// have to get name: String out of FileAttr.
//
// TODO: right now you have to make two calls to "open" a file - a lookup and a
// read - even though we're doing the same btree walk for both. should we bother
// returning the location info in lookup?
//
// TODO: the double-calls for open is even sillier if you're doing a file walk.
// should we put Location in the fileattr thing? should we have some public
// dentry that exposes both metadata and data?

use flatbuffers::{FlatBufferBuilder, WIPOffset};

#[path = "./volume.fbs.rs"]
#[allow(warnings)]
#[rustfmt::skip]
mod fb;

use crate::{ByteRange, FileAttr, FileType, Location};

#[derive(Debug, PartialEq, Eq)]
pub enum VolumeError {
    Invalid { message: Cow<'static, str> },
    NotADirectory,
    IsADirectory,
    DoesNotExist,
    Exists,
}

impl VolumeError {
    fn invalid<S: Into<Cow<'static, str>>>(msg: S) -> Self {
        Self::Invalid {
            message: msg.into(),
        }
    }
}

#[derive(Debug)]
pub struct Volume {
    locations: Vec<Location>,
    // ino -> location key
    data: BTreeMap<u64, Entry>,
    // (parent, name) -> ino
    dirs: BTreeMap<EntryKey<'static>, u64>,
}

#[derive(Debug, Clone)]
struct Entry {
    name: Cow<'static, str>,
    attr: FileAttr,
    parent_ino: u64,
    data: EntryData,
}

impl Entry {
    fn is_dir(&self) -> bool {
        matches!(self.data, EntryData::Directory)
    }
}

#[derive(Clone, Copy, Debug)]
enum EntryData {
    Directory,
    File {
        location_idx: usize,
        byte_range: ByteRange,
    },
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EntryKeyRef<'a> {
    ino: u64,
    name: Cow<'a, str>,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EntryKey<'a>(EntryKeyRef<'a>);

impl<'a> From<(u64, &'a str)> for EntryKey<'a> {
    fn from((ino, name): (u64, &'a str)) -> Self {
        EntryKey(EntryKeyRef {
            ino,
            name: name.into(),
        })
    }
}

impl From<(u64, String)> for EntryKey<'static> {
    fn from((ino, name): (u64, String)) -> Self {
        EntryKey(EntryKeyRef {
            ino,
            name: name.into(),
        })
    }
}

impl<'r, 'k> Borrow<EntryKeyRef<'r>> for EntryKey<'k>
where
    'k: 'r,
{
    fn borrow(&self) -> &EntryKeyRef<'r> {
        &self.0
    }
}

#[allow(unused)]
impl Volume {
    const ROOT: Entry = Entry {
        name: Cow::Borrowed("/"),
        parent_ino: 0,
        attr: FileAttr {
            ino: 0,
            size: 0,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            kind: FileType::Directory,
        },
        data: EntryData::Directory,
    };

    /// Create a new empty volume.
    pub fn empty() -> Self {
        Self {
            locations: vec![],
            data: BTreeMap::new(),
            dirs: BTreeMap::new(),
        }
    }

    /// Create a new directory.
    ///
    /// Returns an error if the parent directory does not exist, or if it
    /// already contains a file or directory with that name.
    pub fn mkdir(&mut self, parent_ino: u64, name: String) -> Result<&FileAttr, VolumeError> {
        // validate that it's okay to create the directory before any state is
        // modified - don't want to undo anything if we can help it
        let _ = self.dir_entry(parent_ino)?;
        let ino = ino(parent_ino, &name);
        let slot = match self.data.entry(ino) {
            btree_map::Entry::Vacant(slot) => slot,
            btree_map::Entry::Occupied(slot) => {
                return if !slot.get().is_dir() {
                    Err(VolumeError::NotADirectory)
                } else {
                    Err(VolumeError::Exists)
                };
            }
        };

        // start modifying things
        let new_entry = Entry {
            name: name.clone().into(),
            parent_ino,
            attr: FileAttr {
                ino,
                size: 0,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                kind: FileType::Directory,
            },
            data: EntryData::Directory,
        };
        let entry = slot.insert(new_entry);
        self.dirs.insert((parent_ino, name).into(), ino);
        Ok(&entry.attr)
    }

    /// Create a sequence of intermediate directories.
    ///
    /// Creation will fail if a file with the same name as an intermediate directory
    /// already exists or if the passed path was empty.
    pub fn mkdir_all(
        &mut self,
        parent_ino: u64,
        dir_names: impl IntoIterator<Item = String>,
    ) -> Result<&FileAttr, VolumeError> {
        let parent = self.dir_entry(parent_ino)?;

        // this is a little bit gross - we should get the final attr out of
        // mkdir but lifetime rules mean we have an annoying time holding onto
        // the returned reference. hack it by just making `dir` an ino and then
        // doing one final lookup at the end.
        //
        // the clone for dir_name when checking EEXIST is a little gross too,
        // and can probably only get fixed by messing with mkdir's return type.
        let mut dir = parent.attr.ino;
        for dir_name in dir_names {
            dir = match self.mkdir(dir, dir_name.clone()) {
                Ok(attr) => attr.ino,
                Err(VolumeError::Exists) => {
                    self.lookup(dir, &dir_name)
                        .expect("lookup after eexists")
                        .ino
                }
                Err(e) => return Err(e),
            }
        }

        // you gotta pass something man
        if dir == 0 {
            return Err(VolumeError::invalid("empty path"));
        }

        Ok(&self.data.get(&dir).unwrap().attr)
    }

    /// Create a file.
    ///
    /// If `exclusive` is set and the file already exists, creation will fail
    /// otherwise any existing file will be overwritten. Creation will always
    /// fail if the parent directory does not exist or a directory with the same
    /// name already exists.
    pub fn create(
        &mut self,
        parent_ino: u64,
        name: String,
        exclusive: bool,
        location: Location,
        byte_range: ByteRange,
    ) -> Result<&FileAttr, VolumeError> {
        // validate that the parent directory exists and we're allowed to
        // create this file (permissions, O_EXCL, etc) before modifying
        // any state
        let _ = self.dir_entry(parent_ino)?;

        let ino = ino(parent_ino, &name);
        let slot = self.data.entry(ino);
        if let btree_map::Entry::Occupied(slot) = &slot {
            if exclusive {
                return Err(VolumeError::Exists);
            }
            if slot.get().is_dir() {
                return Err(VolumeError::IsADirectory);
            }
        }

        // verification is okay, start modifying things
        let location_idx = insert_sorted(&mut self.locations, location);
        let new_entry = Entry {
            name: name.clone().into(),
            parent_ino,
            attr: FileAttr {
                ino,
                size: byte_range.len,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                kind: FileType::Regular,
            },
            data: EntryData::File {
                location_idx,
                byte_range,
            },
        };

        let entry = match slot {
            btree_map::Entry::Vacant(slot) => slot.insert(new_entry),
            btree_map::Entry::Occupied(slot) => {
                let entry = slot.into_mut();
                *entry = new_entry;
                entry
            }
        };
        self.dirs.insert((parent_ino, name).into(), ino);
        Ok(&entry.attr)
    }

    fn insert_location(&mut self, location: Location) -> usize {
        match self.locations.binary_search(&location) {
            Ok(idx) => idx,
            Err(idx) => {
                self.locations.insert(idx, location);
                idx
            }
        }
    }

    /// Lookup a directory entry by name.
    pub fn lookup(&self, parent_ino: u64, name: &str) -> Option<&FileAttr> {
        let k = EntryKey::from((parent_ino, name));
        self.dirs
            .get(&k)
            .and_then(|ino| self.data.get(ino))
            .map(|dent| &dent.attr)
    }

    /// Obtain information about a file based only on its `ino`.
    pub fn stat(&self, ino: u64) -> Option<&FileAttr> {
        self.data.get(&ino).map(|dent| &dent.attr)
    }

    /// Iterate over the entires in a directory. Returns an iterator of
    /// `(filename, attr)` pairs.
    ///
    /// Iterator order is not guaranteed to be stable.
    pub fn readdir<'a>(&'a self, ino: u64) -> Result<ReadDir<'a>, VolumeError> {
        let _ = self.dir_entry(ino)?;

        let start: EntryKey = (ino, "").into();
        let end: EntryKey = (ino + 1, "").into();
        Ok(ReadDir {
            range: self.dirs.range(start..end),
            data: &self.data,
        })
    }

    /// Walk a subtree starting from a directory. Walks are done in depth-first
    /// order, but order of items in a directory is not guaranteed to be stable.
    ///
    /// Returns an iterator over `(filename, ancestors, attrs)` tuples, where
    /// `filename` and `attrs` are the same values that would be yielded from
    /// calling `readdir` on a directory and `ancestors` is a `Vec` of ancestor
    /// directory names.
    pub fn walk<'a>(&'a self, ino: u64) -> Result<WalkIter<'a>, VolumeError> {
        let root_dir = self.dir_entry(ino)?;

        let start: EntryKey = (ino, "").into();
        let end: EntryKey = (ino + 1, "").into();
        let root_iter = ReadDir {
            range: self.dirs.range(start..end),
            data: &self.data,
        };

        Ok(WalkIter {
            volume: self,
            readdirs: vec![root_iter],
            ancestors: Vec::new(),
        })
    }

    #[inline]
    fn dir_entry(&self, ino: u64) -> Result<&Entry, VolumeError> {
        if ino == 0 {
            return Ok(&Self::ROOT);
        }

        let entry = self.data.get(&ino).ok_or(VolumeError::DoesNotExist)?;
        if !entry.is_dir() {
            return Err(VolumeError::NotADirectory);
        }
        Ok(entry)
    }

    /// Get a file's physical location for opening and reading.
    ///
    /// Attempting to get the physical location of a directory or a symlink
    /// returns and error.
    pub fn location(&self, ino: u64) -> Option<(&Location, &ByteRange)> {
        self.data.get(&ino).and_then(|entry| match &entry.data {
            // directories have no location
            EntryData::Directory => None,
            // files need to map to blob list
            EntryData::File {
                location_idx,
                byte_range,
            } => {
                let location = self.locations.get(*location_idx)?;
                Some((location, byte_range))
            }
        })
    }

    /// Serialize this volume to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut fbb = FlatBufferBuilder::new();

        let locations = {
            let mut locations = Vec::with_capacity(self.locations.len());
            for location in &self.locations {
                locations.push(to_fb_location(&mut fbb, location))
            }
            fbb.create_vector(&locations)
        };
        let entries = {
            let mut dir_entries = Vec::with_capacity(self.data.len());
            for entry in self.data.values() {
                dir_entries.push(to_fb_entry(&mut fbb, entry));
            }
            fbb.create_vector(&dir_entries)
        };

        let volume = fb::Volume::create(
            &mut fbb,
            &fb::VolumeArgs {
                version: 0xBEEF,
                locations: Some(locations),
                entries: Some(entries),
            },
        );
        fbb.finish(volume, None);
        fbb.finished_data().into()
    }

    /// Read a serialized volume. Returns an error if the volume is invalid or
    /// inconsistent.
    pub fn from_bytes(bs: &[u8]) -> Result<Self, VolumeError> {
        let fb_volume =
            fb::root_as_volume(bs).map_err(|_| VolumeError::invalid("invalid bytes"))?;

        if fb_volume.version() != 0xBEEF {
            return Err(VolumeError::invalid("invalid version"));
        }

        let locations = fb_volume
            .locations()
            .iter()
            .map(from_fb_location)
            .collect::<Result<Vec<_>, _>>()?;

        let mut data = BTreeMap::new();
        let mut entries = BTreeMap::new();
        for entry in fb_volume.entries().iter() {
            let k = (entry.parent_ino(), entry.name().to_string()).into();
            let entry = from_fb_entry(&entry)?;
            entries.insert(k, entry.attr.ino);
            data.insert(entry.attr.ino, entry);
        }

        Ok(Self {
            locations,
            data,
            dirs: entries,
        })
    }
}

/// The iterator returned from [readdir][Volume::readdir].
pub struct ReadDir<'a> {
    data: &'a BTreeMap<u64, Entry>,
    range: btree_map::Range<'a, EntryKey<'static>, u64>,
}

impl<'a> Iterator for ReadDir<'a> {
    type Item = (&'a str, &'a FileAttr);

    fn next(&mut self) -> Option<Self::Item> {
        self.range.next().map(|(EntryKey(k), ino)| {
            let dent = self.data.get(ino).unwrap();
            (k.name.as_ref(), &dent.attr)
        })
    }
}

/// The iterator returned from [walk][Volume::walk].
pub struct WalkIter<'a> {
    volume: &'a Volume,

    // a stack of entry iterators
    readdirs: Vec<ReadDir<'a>>,

    // the names of all the directories opened to get here
    ancestors: Vec<&'a str>,
}

impl<'a> Iterator for WalkIter<'a> {
    // TODO: it's a big gnarly to be cloning and returning the ancestors path
    // every time but the lifetime on returning a slice referencing self
    // is a pain to express.
    type Item = (&'a str, Vec<&'a str>, &'a FileAttr);

    fn next(&mut self) -> Option<Self::Item> {
        while !self.readdirs.is_empty() {
            let next = self.readdirs.last_mut().unwrap().next();
            match next {
                Some((name, attr)) => {
                    let ancestors = if attr.is_directory() {
                        let next = self.volume.readdir(attr.ino).unwrap();
                        let ancestors = self.ancestors.clone();
                        self.readdirs.push(next);
                        self.ancestors.push(name);
                        ancestors
                    } else {
                        self.ancestors.clone()
                    };
                    return Some((name, ancestors, attr));
                }
                None => {
                    self.readdirs.pop();
                    self.ancestors.pop();
                }
            }
        }

        None
    }
}

fn ino(parent_ino: u64, name: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    (parent_ino, &name).hash(&mut hasher);
    hasher.finish()
}

fn insert_sorted<T: Ord>(vec: &mut Vec<T>, location: T) -> usize {
    match vec.binary_search(&location) {
        Ok(idx) => idx,
        Err(idx) => {
            vec.insert(idx, location);
            idx
        }
    }
}

fn to_fb_entry<'a>(fbb: &mut FlatBufferBuilder<'a>, entry: &Entry) -> WIPOffset<fb::Entry<'a>> {
    let attrs = fb::FileAttrs::create(
        fbb,
        &fb::FileAttrsArgs {
            ino: entry.attr.ino,
            size: entry.attr.size,
            mtime: Some(&entry.attr.mtime.into()),
            ctime: Some(&entry.attr.ctime.into()),
            kind: entry.attr.kind.into(),
        },
    );
    let location_ref = match &entry.data {
        EntryData::File {
            location_idx,
            byte_range,
        } => Some({
            let byte_range = fb::ByteRange::new(byte_range.offset, byte_range.len);
            fb::LocationRef::create(
                fbb,
                &fb::LocationRefArgs {
                    location_index: *location_idx as u16,
                    byte_range: Some(&byte_range),
                },
            )
        }),
        _ => None,
    };

    let name = fbb.create_string(&entry.name);
    fb::Entry::create(
        fbb,
        &fb::EntryArgs {
            name: Some(name),
            parent_ino: entry.parent_ino,
            attrs: Some(attrs),
            location_ref,
        },
    )
}

fn from_fb_entry(fb_entry: &fb::Entry) -> Result<Entry, VolumeError> {
    let name = fb_entry.name().to_string().into();
    let parent_ino = fb_entry.parent_ino();
    let attr = {
        let fb_attr = fb_entry.attrs();
        FileAttr {
            ino: fb_attr.ino(),
            size: fb_attr.size(),
            mtime: fb_attr.mtime().into(),
            ctime: fb_attr.ctime().into(),
            kind: fb_attr.kind().try_into()?,
        }
    };

    let data = match attr.kind {
        FileType::Regular => {
            let Some(location_ref) = fb_entry.location_ref() else {
                return Err(VolumeError::invalid("missing file data pointer"));
            };
            let Some(byte_range) = location_ref.byte_range() else {
                return Err(VolumeError::invalid("missing file data range"));
            };
            EntryData::File {
                location_idx: location_ref.location_index() as usize,
                byte_range: ByteRange {
                    offset: byte_range.offset(),
                    len: byte_range.length(),
                },
            }
        }
        FileType::Directory => EntryData::Directory,
        FileType::Symlink => return Err(VolumeError::invalid("symlinks are not yet supported")),
    };

    Ok(Entry {
        name,
        attr,
        parent_ino,
        data,
    })
}

impl From<SystemTime> for fb::Timespec {
    fn from(time: SystemTime) -> Self {
        let since_epoch = time
            .duration_since(UNIX_EPOCH)
            .expect("time before unix epoch");
        fb::Timespec::new(since_epoch.as_secs(), since_epoch.subsec_nanos())
    }
}

impl From<&fb::Timespec> for SystemTime {
    fn from(time: &fb::Timespec) -> Self {
        UNIX_EPOCH + Duration::new(time.sec(), time.nsec())
    }
}

impl From<FileType> for fb::FileType {
    fn from(ft: FileType) -> Self {
        match ft {
            FileType::Regular => fb::FileType::Regular,
            FileType::Directory => fb::FileType::Directory,
            FileType::Symlink => fb::FileType::Symlink,
        }
    }
}

impl TryFrom<fb::FileType> for FileType {
    type Error = VolumeError;

    fn try_from(ft: fb::FileType) -> Result<Self, Self::Error> {
        match ft {
            fb::FileType::Regular => Ok(FileType::Regular),
            fb::FileType::Directory => Ok(FileType::Directory),
            fb::FileType::Symlink => Ok(FileType::Symlink),
            ft => Err(VolumeError::invalid(format!(
                "unknown file type: code={code}, name={name}",
                code = ft.0,
                name = ft.variant_name().unwrap_or("")
            ))),
        }
    }
}

fn to_fb_location<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    location: &Location,
) -> WIPOffset<fb::LocationWrapper<'a>> {
    let union = match location {
        Location::Local(path_buf) => {
            // FIXME: what do we do about non-utf8 paths?
            let path = fbb.create_string(path_buf.to_str().unwrap());
            let location =
                fb::LocalLocation::create(fbb, &fb::LocalLocationArgs { path: Some(path) });
            location.as_union_value()
        }
        Location::ObjectStorage { bucket, key } => {
            let bucket = fbb.create_shared_string(bucket);
            let key = fbb.create_string(key);
            let location = fb::S3Location::create(
                fbb,
                &fb::S3LocationArgs {
                    bucket: Some(bucket),
                    key: Some(key),
                },
            );
            location.as_union_value()
        }
    };

    fb::LocationWrapper::create(
        fbb,
        &fb::LocationWrapperArgs {
            location: Some(union),
            location_type: match location {
                Location::Local(_) => fb::Location::local,
                Location::ObjectStorage { .. } => fb::Location::s3,
            },
        },
    )
}

fn from_fb_location(fb_location: fb::LocationWrapper) -> Result<Location, VolumeError> {
    match fb_location.location_type() {
        fb::Location::local => {
            let fb_location = fb_location.location_as_local().unwrap();
            let path = PathBuf::from(fb_location.path());
            Ok(Location::Local(path))
        }
        fb::Location::s3 => {
            let fb_location = fb_location.location_as_s_3().unwrap();
            let bucket = fb_location.bucket().to_string();
            let key = fb_location.key().to_string();
            Ok(Location::ObjectStorage { bucket, key })
        }
        lt => Err(VolumeError::invalid(format!(
            "unknown location type: code={code} name={name}",
            code = lt.0,
            name = lt.variant_name().unwrap_or("")
        ))),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_volume_lookup() {
        let mut volume = Volume::empty();
        let a = volume.mkdir(0, "a".to_string()).unwrap().clone();
        let b = volume.mkdir(a.ino, "b".to_string()).unwrap().clone();
        let c = volume.mkdir(b.ino, "c".to_string()).unwrap().clone();

        volume
            .create(
                c.ino,
                "test.txt".to_string(),
                true,
                Location::ObjectStorage {
                    bucket: "test-bucket".to_string(),
                    key: "test.txt".to_string(),
                },
                ByteRange { offset: 0, len: 64 },
            )
            .unwrap();

        assert_eq!(
            volume
                .readdir(0)
                .unwrap()
                .map(|(n, _attr)| n)
                .collect::<Vec<_>>(),
            vec!["a"],
        );
        assert_eq!(
            volume
                .readdir(a.ino)
                .unwrap()
                .map(|(n, _attr)| n)
                .collect::<Vec<_>>(),
            vec!["b"],
        );
        assert_eq!(
            volume
                .readdir(b.ino)
                .unwrap()
                .map(|(n, _attr)| n)
                .collect::<Vec<_>>(),
            vec!["c"],
        );

        assert_read_test_txt(&volume);
    }

    #[test]
    fn mkdir_all() {
        let mut volume = Volume::empty();
        let a = volume.mkdir(0, "a".to_string()).unwrap().clone();
        assert!(a.is_directory());

        let b = volume
            .mkdir_all(0, ["a".to_string(), "b".to_string()])
            .unwrap()
            .clone();
        assert!(b.is_directory());
        assert_eq!(volume.lookup(a.ino, "b").unwrap().ino, b.ino);

        let c = volume
            .mkdir_all(0, ["a".to_string(), "b".to_string(), "c".to_string()])
            .unwrap()
            .clone();
        assert!(c.is_directory());
        assert_eq!(volume.lookup(b.ino, "c").unwrap().ino, c.ino);

        // try a partially overlapping directory
        volume
            .mkdir_all(
                a.ino,
                ["b".to_string(), "potato".to_string(), "tomato".to_string()],
            )
            .unwrap();
        let potato = volume.lookup(b.ino, "potato").unwrap().clone();
        let tomato = volume.lookup(potato.ino, "tomato").unwrap().clone();
        assert!(tomato.is_directory());
    }

    #[test]
    fn walk() {
        let mut volume = Volume::empty();
        let b = volume
            .mkdir_all(0, ["a".to_string(), "b".to_string()])
            .unwrap()
            .clone();
        volume
            .create(
                b.ino,
                "c.txt".to_string(),
                true,
                Location::Local("foo.txt".into()),
                (0, 10).into(),
            )
            .unwrap();

        let c = volume
            .mkdir_all(0, ["a".to_string(), "c".to_string(), "e".to_string()])
            .unwrap()
            .clone();
        volume
            .create(
                c.ino,
                "f.txt".to_string(),
                true,
                Location::Local("foo.bin".into()),
                (0, 10).into(),
            )
            .unwrap();

        let names: Vec<_> = volume
            .walk(0)
            .unwrap()
            .map(|(name, mut dirs, _)| {
                dirs.push(name);
                dirs.join("/")
            })
            .collect();
        assert_eq!(
            names,
            vec!["a", "a/b", "a/b/c.txt", "a/c", "a/c/e", "a/c/e/f.txt"],
        )
    }

    #[test]
    fn test_volume_read_write() {
        let mut volume = Volume::empty();
        let a = volume.mkdir(0, "a".to_string()).unwrap().clone();
        let b = volume.mkdir(a.ino, "b".to_string()).unwrap().clone();
        let c = volume.mkdir(b.ino, "c".to_string()).unwrap().clone();
        volume
            .create(
                c.ino,
                "test.txt".to_string(),
                true,
                Location::ObjectStorage {
                    bucket: "test-bucket".to_string(),
                    key: "test.txt".to_string(),
                },
                ByteRange { offset: 0, len: 64 },
            )
            .unwrap();

        let bs = volume.to_bytes();
        assert_read_test_txt(&Volume::from_bytes(&bs).unwrap());
    }

    fn assert_read_test_txt(volume: &Volume) {
        let a = volume.lookup(0, "a").unwrap().clone();
        let b = volume.lookup(a.ino, "b").unwrap().clone();
        let c = volume.lookup(b.ino, "c").unwrap().clone();
        let test_txt = volume.lookup(c.ino, "test.txt").unwrap().clone();

        assert_eq!(
            volume.location(test_txt.ino).unwrap(),
            (
                &Location::ObjectStorage {
                    bucket: "test-bucket".to_string(),
                    key: "test.txt".to_string(),
                },
                &ByteRange { offset: 0, len: 64 },
            )
        );
    }
}
