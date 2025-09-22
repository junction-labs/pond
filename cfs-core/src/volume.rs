use std::{
    borrow::{Borrow, Cow},
    collections::{BTreeMap, btree_map},
    path::PathBuf,
    sync::Arc,
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

use crate::{ByteRange, FileAttr, FileType, Ino, Location};

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum VolumeError {
    #[error("not a directory")]
    NotADirectory,
    #[error("is a directory")]
    IsADirectory,
    #[error("does not exist")]
    DoesNotExist,
    #[error("already exists")]
    AlreadyExists,
    #[error("{message}")]
    Invalid { message: Cow<'static, str> },
}

impl VolumeError {
    fn invalid<S: Into<Cow<'static, str>>>(msg: S) -> Self {
        Self::Invalid {
            message: msg.into(),
        }
    }
}

/// A Volume holds all of file and directory metadata for a coolfs volume. Volumes
/// are extremely low-level, and working with a volume requires understanding the
/// structure of your filesystem.
///
/// # Staging
///
/// Existing Volumes can be mutated and modified to create a new version of the
/// same volume. Volumes can be used for metdaddata bookeeping while building
/// new physical storage by passing a [Staged location][Location] when creating
/// or updating files.
///
/// A staged location allows creating a blob locally, uploading it to object
/// storage, and then finalizing a volume. It does not help when relocating an
/// object inside a physical blob.
///
/// Because staged locations are effectively dangling pointers, volumes cannot
/// be serialized while they're being staged.
//
// # TODO: should we guarantee inodes are stable in the docs?
#[derive(Debug)]
pub struct Volume {
    version: u64,
    version_bytes: Arc<[u8]>,

    // the next availble ino. must start at Ino::Root.add(1) for an empty
    // volume.
    next_ino: Ino,

    // interned list of locations that hold data blobs. this list must
    // remain in a stable order. each Entry contains indexes into this
    // vec, and re-ordering it invalidates those indexes.
    locations: Vec<Location>,

    // Ino -> Entry
    data: BTreeMap<Ino, Entry>,

    // (Parent, Name) -> Ino
    //
    // a secondary index that represents directory entires. can be reconstructed
    // from the parent Ino listed in Entry.
    dirs: BTreeMap<EntryKey<'static>, Ino>,
}

#[derive(Debug, Clone)]
struct Entry {
    name: Cow<'static, str>,
    attr: FileAttr,
    parent: Ino,
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
    Dynamic,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EntryKeyRef<'a> {
    ino: Ino,
    name: Cow<'a, str>,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EntryKey<'a>(EntryKeyRef<'a>);

impl<'a> From<(Ino, &'a str)> for EntryKey<'a> {
    fn from((ino, name): (Ino, &'a str)) -> Self {
        EntryKey(EntryKeyRef {
            ino,
            name: name.into(),
        })
    }
}

impl From<(Ino, String)> for EntryKey<'static> {
    fn from((ino, name): (Ino, String)) -> Self {
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
    /// Create a new empty volume.
    pub fn empty() -> Self {
        let mut data = BTreeMap::new();
        let mut dirs = BTreeMap::new();
        for entry in Self::reserved_entries() {
            if !entry.attr.ino.is_root() {
                let dir_key = (Ino::Root, entry.name.to_string()).into();
                dirs.insert(dir_key, entry.attr.ino);
            }
            data.insert(entry.attr.ino, entry);
        }

        Self {
            version: 0xBEEF,
            version_bytes: Arc::from(format!("{:#x}", 0xBEEF).into_bytes().as_slice()),
            next_ino: Ino::min_regular(),
            locations: vec![],
            data,
            dirs,
        }
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn version_data(&self) -> Arc<[u8]> {
        self.version_bytes.clone()
    }

    const fn reserved_entries() -> [Entry; 2] {
        [
            Entry {
                name: Cow::Borrowed("/"),
                parent: Ino::Root,
                attr: FileAttr {
                    ino: Ino::Root,
                    size: 0,
                    mtime: UNIX_EPOCH,
                    ctime: UNIX_EPOCH,
                    kind: FileType::Directory,
                },
                data: EntryData::Directory,
            },
            Entry {
                name: Cow::Borrowed(".version"),
                parent: Ino::Root,
                attr: FileAttr {
                    ino: Ino::Reserved(2),
                    size: 0,
                    mtime: UNIX_EPOCH,
                    ctime: UNIX_EPOCH,
                    kind: FileType::Regular,
                },
                data: EntryData::Dynamic,
            },
        ]
    }

    /// Check whether this volume is being staged. Staged volumes contain
    /// unstable data references and can't be serialized.
    pub fn is_staged(&self) -> bool {
        self.locations
            .iter()
            .any(|l| matches!(l, Location::Staged(_)))
    }

    /// Change the phyical location of a data blob.
    ///
    /// This is most often used when rewriting a volume to change data locations.
    ///
    /// ```no_run
    /// # use cfs_core::{ByteRange, Ino, Location, volume::{Volume, VolumeError}};
    /// # use std::path::PathBuf;
    /// # fn doc() -> Result<(), VolumeError> {
    /// let mut volume = Volume::empty();
    ///
    /// volume.create(
    ///     Ino::Root,
    ///     "test.txt".to_string(),
    ///     true,
    ///     Location::Staged(0),
    ///     ByteRange { offset: 0, len: 64 },
    /// )?;
    ///
    /// volume.relocate(&Location::Staged(0), Location::Local {
    ///   path: PathBuf::from("./test.txt"),
    ///   len: 64,
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn relocate(&mut self, from: &Location, to: Location) -> Result<(), VolumeError> {
        let Some(from) = self.locations.iter_mut().find(|l| l == &from) else {
            return Err(VolumeError::invalid("unknown location"));
        };
        *from = to;
        Ok(())
    }

    /// Create a new directory.
    ///
    /// Returns an error if the parent directory does not exist, or if it
    /// already contains a file or directory with that name.
    pub fn mkdir(&mut self, parent: Ino, name: String) -> Result<&FileAttr, VolumeError> {
        // validate that it's okay to create the directory before any state is
        // modified - don't want to undo anything if we can help it
        //
        // lookup checks parent is a directory.
        let _ = self.dir_entry(parent)?;
        if self.lookup_unchecked(parent, &name).is_some() {
            return Err(VolumeError::AlreadyExists);
        }

        // start modifying things
        let ino = self.next_ino();
        let slot = match self.data.entry(ino) {
            btree_map::Entry::Vacant(slot) => slot,
            btree_map::Entry::Occupied(slot) => unreachable!("BUG: inode reused"),
        };
        let new_entry = Entry {
            name: name.clone().into(),
            parent,
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
        self.dirs.insert(EntryKey::from((parent, name)), ino);
        Ok(&entry.attr)
    }

    /// Create a sequence of intermediate directories.
    ///
    /// Creation will fail if a file with the same name as an intermediate directory
    /// already exists or if the passed path was empty.
    pub fn mkdir_all(
        &mut self,
        parent: Ino,
        dir_names: impl IntoIterator<Item = String>,
    ) -> Result<&FileAttr, VolumeError> {
        let parent = self.dir_entry(parent)?;

        // this is a little bit gross - we should get the final attr out of
        // mkdir but lifetime rules mean we have an annoying time holding onto
        // the returned reference. hack it by just making `dir` an ino and then
        // doing one final lookup at the end.
        //
        // the clone for dir_name when checking EEXIST is a little gross too,
        // and can probably only get fixed by messing with mkdir's return type.
        let mut dir = parent.attr.ino;
        let mut depth = 0;
        for dir_name in dir_names {
            depth += 1;
            dir = match self.mkdir(dir, dir_name.clone()) {
                Ok(attr) => attr.ino,
                Err(VolumeError::AlreadyExists) => {
                    self.lookup_unchecked(dir, &dir_name)
                        .expect("BUG: lookup failed after exists error")
                        .ino
                }
                Err(e) => return Err(e),
            }
        }

        // you gotta pass something man
        if depth == 0 {
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
        parent: Ino,
        name: String,
        exclusive: bool,
        location: Location,
        byte_range: ByteRange,
    ) -> Result<&FileAttr, VolumeError> {
        // validate that the parent directory exists and we're allowed to
        // create this file (permissions, O_EXCL, etc) before modifying
        // any state
        let _ = self.dir_entry(parent)?;
        match self.lookup_unchecked(parent, &name) {
            Some(e) if exclusive => return Err(VolumeError::AlreadyExists),
            Some(e) if e.is_directory() => return Err(VolumeError::IsADirectory),
            _ => (), // ok!
        }

        // verification is okay, start modifying things
        let ino = self.next_ino();
        let slot = self.data.entry(ino);

        let location_idx = insert_unique(&mut self.locations, location);
        let new_entry = Entry {
            name: name.clone().into(),
            parent,
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
        self.dirs.insert((parent, name).into(), ino);
        Ok(&entry.attr)
    }

    fn next_ino(&mut self) -> Ino {
        let ino = self.next_ino;
        self.next_ino = self.next_ino.add(1);
        ino
    }

    /// Lookup a directory entry by name.
    pub fn lookup(&self, parent: Ino, name: &str) -> Result<Option<&FileAttr>, VolumeError> {
        let _ = self.dir_entry(parent)?;

        Ok(self.lookup_unchecked(parent, name))
    }

    #[inline]
    fn lookup_unchecked(&self, parent: Ino, name: &str) -> Option<&FileAttr> {
        let k = EntryKey::from((parent, name));

        self.dirs
            .get(&k)
            .and_then(|ino| self.data.get(ino))
            .map(|dent| &dent.attr)
    }

    /// Obtain information about a file based only on its `ino`.
    pub fn stat(&self, ino: Ino) -> Option<&FileAttr> {
        self.data.get(&ino.into()).map(|dent| &dent.attr)
    }

    /// Iterate over the entires in a directory. Returns an iterator of
    /// `(filename, attr)` pairs.
    ///
    /// Iterator order is not guaranteed to be stable.
    pub fn readdir<'a>(&'a self, ino: Ino) -> Result<ReadDir<'a>, VolumeError> {
        let _ = self.dir_entry(ino)?;
        Ok(self.dir_iter(ino))
    }

    /// Walk a subtree starting from a directory. Walks are done in depth-first
    /// order, but order of items in a directory is not guaranteed to be stable.
    ///
    /// Returns an iterator over `(filename, ancestors, attrs)` tuples, where
    /// `filename` and `attrs` are the same values that would be yielded from
    /// calling `readdir` on a directory and `ancestors` is a `Vec` of ancestor
    /// directory names.
    pub fn walk<'a>(&'a self, ino: Ino) -> Result<WalkIter<'a>, VolumeError> {
        let root_dir = self.dir_entry(ino)?;
        let root_iter = self.dir_iter(ino);
        Ok(WalkIter {
            volume: self,
            readdirs: vec![root_iter],
            ancestors: Vec::new(),
        })
    }

    #[inline]
    fn dir_entry(&self, ino: Ino) -> Result<&Entry, VolumeError> {
        let entry = self.data.get(&ino).ok_or(VolumeError::DoesNotExist)?;
        if !entry.is_dir() {
            return Err(VolumeError::NotADirectory);
        }
        Ok(entry)
    }

    fn dir_iter(&'_ self, ino: Ino) -> ReadDir<'_> {
        let start: EntryKey = (ino, "").into();
        let end: EntryKey = (ino.add(1), "").into();
        ReadDir {
            range: self.dirs.range(start..end),
            data: &self.data,
        }
    }

    /// Get a file's physical location for opening and reading.
    ///
    /// Attempting to get the physical location of a directory or a symlink
    /// returns an error.
    pub fn location(&self, ino: Ino) -> Option<(&Location, &ByteRange)> {
        self.data.get(&ino).and_then(|entry| match &entry.data {
            // files need to map to blob list
            EntryData::File {
                location_idx,
                byte_range,
            } => {
                let location = self.locations.get(*location_idx)?;
                Some((location, byte_range))
            }
            // no other file type has a location
            _ => None,
        })
    }

    /// Serialize this volume to bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, VolumeError> {
        let mut fbb = FlatBufferBuilder::new();

        let locations = {
            let mut locations = Vec::with_capacity(self.locations.len());
            for location in &self.locations {
                locations.push(to_fb_location(&mut fbb, location)?)
            }
            fbb.create_vector(&locations)
        };
        let entries = {
            let mut dir_entries = Vec::with_capacity(self.data.len());
            for (ino, entry) in &self.data {
                if !ino.is_regular() {
                    continue;
                }
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
        Ok(fbb.finished_data().into())
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

        if fb_volume.entries().is_empty() {
            return Err(VolumeError::invalid("no entries"));
        }

        // let mut data = BTreeMap::new();
        // let mut entries = BTreeMap::new();
        let mut max_ino = Ino::Root;
        let mut volume = Self::empty();

        // set the locations
        volume.locations = locations;

        // walk the serialized entries and insert both the entries
        // and their dir index entry
        for entry in fb_volume.entries().iter() {
            let parent_ino = entry.parent_ino().into();
            let dir_key = (parent_ino, entry.name().to_string()).into();
            let entry = from_fb_entry(&entry)?;
            max_ino = max_ino.max(entry.attr.ino);
            volume.dirs.insert(dir_key, entry.attr.ino);
            volume.data.insert(entry.attr.ino, entry);
        }

        Ok(volume)
    }
}

fn insert_unique<T: std::cmp::PartialEq>(xs: &mut Vec<T>, x: T) -> usize {
    match xs.iter().position(|e| e == &x) {
        Some(idx) => idx,
        None => {
            let idx = xs.len();
            xs.push(x);
            idx
        }
    }
}

/// The iterator returned from [readdir][Volume::readdir].
pub struct ReadDir<'a> {
    data: &'a BTreeMap<Ino, Entry>,
    range: btree_map::Range<'a, EntryKey<'static>, Ino>,
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

fn to_fb_entry<'a>(fbb: &mut FlatBufferBuilder<'a>, entry: &Entry) -> WIPOffset<fb::Entry<'a>> {
    let attrs = fb::FileAttrs::create(
        fbb,
        &fb::FileAttrsArgs {
            ino: entry.attr.ino.into(),
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
            parent_ino: entry.parent.into(),
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
            ino: fb_attr.ino().into(),
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
        parent: parent_ino.into(),
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
) -> Result<WIPOffset<fb::LocationWrapper<'a>>, VolumeError> {
    let union = match location {
        Location::Local { path, len } => {
            // FIXME: what do we do about non-utf8 paths?
            let path = fbb.create_string(path.to_str().unwrap());
            let location = fb::LocalLocation::create(
                fbb,
                &fb::LocalLocationArgs {
                    path: Some(path),
                    length: *len as u64,
                },
            );
            location.as_union_value()
        }
        Location::ObjectStorage { bucket, key, len } => {
            let bucket = fbb.create_shared_string(bucket);
            let key = fbb.create_string(key);
            let location = fb::S3Location::create(
                fbb,
                &fb::S3LocationArgs {
                    bucket: Some(bucket),
                    key: Some(key),
                    length: *len as u64,
                },
            );
            location.as_union_value()
        }
        _ => {
            return Err(VolumeError::invalid(
                "can't serialize a volume with staged data",
            ));
        }
    };

    let location_type = match location {
        Location::Local { .. } => fb::Location::local,
        Location::ObjectStorage { .. } => fb::Location::s3,
        _ => panic!("BUG: unknown location type"),
    };

    Ok(fb::LocationWrapper::create(
        fbb,
        &fb::LocationWrapperArgs {
            location: Some(union),
            location_type,
        },
    ))
}

fn from_fb_location(fb_location: fb::LocationWrapper) -> Result<Location, VolumeError> {
    match fb_location.location_type() {
        fb::Location::local => {
            let fb_location = fb_location.location_as_local().unwrap();
            let path = PathBuf::from(fb_location.path());
            let len = fb_location
                .length()
                .try_into()
                .map_err(|_| VolumeError::invalid("length overflow"))?;
            Ok(Location::Local { path, len })
        }
        fb::Location::s3 => {
            let fb_location = fb_location.location_as_s_3().unwrap();
            let bucket = fb_location.bucket().to_string();
            let key = fb_location.key().to_string();
            let len = fb_location
                .length()
                .try_into()
                .map_err(|_| VolumeError::invalid("length overflow"))?;
            Ok(Location::ObjectStorage { bucket, key, len })
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
    fn create() {
        let mut volume = Volume::empty();
        let f1 = volume
            .create(
                Ino::Root,
                "zzzz".to_string(),
                true,
                Location::Local {
                    path: PathBuf::from("zzzz"),
                    len: 123,
                },
                ByteRange {
                    offset: 0,
                    len: 123,
                },
            )
            .unwrap()
            .clone();
        // location should match what we just created with
        let (l1, _) = volume.location(f1.ino).unwrap();
        assert_eq!(local_path(l1), Some("zzzz"));

        let f2 = volume
            .create(
                Ino::Root,
                "aaaa".to_string(),
                true,
                Location::Local {
                    path: PathBuf::from("aaaa"),
                    len: 123,
                },
                ByteRange {
                    offset: 0,
                    len: 123,
                },
            )
            .unwrap()
            .clone();

        // old locations should be stable
        let (l1, _) = volume.location(f1.ino).unwrap();
        assert_eq!(local_path(l1), Some("zzzz"));
        // location should match what we just created with
        let (l2, _) = volume.location(f2.ino).unwrap();
        assert_eq!(local_path(l2), Some("aaaa"));
    }

    fn local_path(l: &Location) -> Option<&str> {
        match l {
            Location::Local { path, .. } => path.as_os_str().to_str(),
            _ => None,
        }
    }

    #[test]
    fn lookup() {
        let mut volume = Volume::empty();
        let a = volume.mkdir(Ino::Root, "a".to_string()).unwrap().clone();
        let b = volume.mkdir(a.ino, "b".to_string()).unwrap().clone();
        let c = volume.mkdir(b.ino, "c".to_string()).unwrap().clone();

        volume
            .create(
                c.ino,
                "test.txt".to_string(),
                true,
                test_location(),
                ByteRange { offset: 0, len: 64 },
            )
            .unwrap();

        // the root directory has some special files. ignore them.
        assert!(
            volume
                .readdir(Ino::Root)
                .unwrap()
                .find(|(name, attr)| *name == "a" && attr.is_directory())
                .is_some()
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
        let a = volume.mkdir(Ino::Root, "a".to_string()).unwrap().clone();
        assert!(a.is_directory());

        let b = volume
            .mkdir_all(Ino::Root, ["a".to_string(), "b".to_string()])
            .unwrap()
            .clone();
        assert!(b.is_directory());
        assert_eq!(assert_lookup(&volume, a.ino, "b").ino, b.ino);

        let c = volume
            .mkdir_all(
                Ino::Root,
                ["a".to_string(), "b".to_string(), "c".to_string()],
            )
            .unwrap()
            .clone();
        assert!(c.is_directory());
        assert_eq!(assert_lookup(&volume, b.ino, "c").ino, c.ino);

        // try a partially overlapping directory
        volume
            .mkdir_all(
                a.ino,
                ["b".to_string(), "potato".to_string(), "tomato".to_string()],
            )
            .unwrap();
        let potato = assert_lookup(&volume, b.ino, "potato");
        let tomato = assert_lookup(&volume, potato.ino, "tomato");
        assert!(tomato.is_directory());
    }

    #[test]
    fn walk() {
        fn all_paths(volume: &Volume) -> Vec<String> {
            volume
                .walk(Ino::Root)
                .unwrap()
                .filter_map(|(name, mut dirs, attr)| {
                    // skip any file at the root of the directory with size
                    // zero. those are special files
                    if dirs.is_empty() && attr.is_file() && attr.size == 0 {
                        None
                    } else {
                        dirs.push(name);
                        Some(dirs.join("/"))
                    }
                })
                .collect()
        }

        let mut volume = Volume::empty();
        assert!(all_paths(&volume).is_empty());

        let b = volume
            .mkdir_all(Ino::Root, ["a".to_string(), "b".to_string()])
            .unwrap()
            .clone();
        volume
            .create(
                b.ino,
                "c.txt".to_string(),
                true,
                test_location(),
                (0, 10).into(),
            )
            .unwrap();

        let c = volume
            .mkdir_all(
                Ino::Root,
                ["a".to_string(), "c".to_string(), "e".to_string()],
            )
            .unwrap()
            .clone();
        volume
            .create(
                c.ino,
                "f.txt".to_string(),
                true,
                test_location(),
                (0, 10).into(),
            )
            .unwrap();

        assert_eq!(
            all_paths(&volume),
            vec!["a", "a/b", "a/b/c.txt", "a/c", "a/c/e", "a/c/e/f.txt"],
        )
    }

    #[test]
    fn volume_to_from_bytes() {
        let mut volume = Volume::empty();
        let a = volume.mkdir(Ino::Root, "a".to_string()).unwrap().clone();
        let b = volume.mkdir(a.ino, "b".to_string()).unwrap().clone();
        let c = volume.mkdir(b.ino, "c".to_string()).unwrap().clone();
        volume
            .create(
                c.ino,
                "test.txt".to_string(),
                true,
                test_location(),
                ByteRange { offset: 0, len: 64 },
            )
            .unwrap();

        let bs = volume.to_bytes().unwrap();
        assert_read_test_txt(&Volume::from_bytes(&bs).unwrap());
    }

    fn assert_lookup(v: &Volume, ino: Ino, name: &str) -> FileAttr {
        v.lookup(ino, name).unwrap().unwrap().clone()
    }

    fn assert_read_test_txt(volume: &Volume) {
        let a = assert_lookup(volume, Ino::Root, "a");
        let b = assert_lookup(volume, a.ino, "b");
        let c = assert_lookup(volume, b.ino, "c");
        let test_txt = assert_lookup(volume, c.ino, "test.txt");

        assert_eq!(
            volume.location(test_txt.ino).unwrap(),
            (&test_location(), &ByteRange { offset: 0, len: 64 },)
        );
    }

    fn test_location() -> Location {
        Location::ObjectStorage {
            bucket: "test-bucket".to_string(),
            key: "test-key.txt".to_string(),
            len: 123,
        }
    }
}
