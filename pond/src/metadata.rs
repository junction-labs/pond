use std::{
    borrow::{Borrow, Cow},
    collections::{BTreeMap, btree_map},
    fmt::Debug,
    path::PathBuf,
    str::FromStr,
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

#[path = "./metadata.fbs.rs"]
#[allow(warnings, clippy::unwrap_used)]
#[rustfmt::skip]
mod fb;

use crate::{ByteRange, Error, FileAttr, FileType, Ino, Location, error::ErrorKind};

/// An arbitrary sequence of bytes that identifies a version of a volume.
///
/// Versions are always exactly 64 bytes, with the high bits set to zero
/// if unspecified.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version(Box<str>);

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<[u8]> for Version {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl AsRef<str> for Version {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl FromStr for Version {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() > Self::MAX_LEN {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "version must be no more than 64 bytes",
            ));
        }

        Ok(Self(Box::from(s)))
    }
}

impl Version {
    const MAX_LEN: usize = 64;

    #[inline(always)]
    fn empty() -> Self {
        Self(Box::from(""))
    }

    /// Create a `Version` from a static string, but panic if the input is
    /// invalid in any way.
    ///
    /// This is equivalent to `from_str`. Prefer that method outside of tests.
    pub fn from_static(version: &'static str) -> Self {
        Self::from_str(version).expect("BUG: invalid version")
    }

    /// Create a `Version` from a slice of bytes.
    pub fn from_bytes(bs: &[u8]) -> crate::Result<Self> {
        let Ok(str) = str::from_utf8(bs) else {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "version must be valid utf-8",
            ));
        };
        Self::from_str(str)
    }
}

/// `VolumeMetadata` holds all of file and directory metadata for a pond
/// volume. Metadata is extremely low-level, and working with a it requires
/// knowledge of the internals of Pond. This should never be the first thing
/// you reach for.
///
/// # Staging
///
/// Existing Volumes can be mutated and modified to create a new version of the
/// same volume. Volumes can be used for metadata bookeeping while building
/// new physical storage by passing a [Staged location][Location] when creating
/// or updating files.
///
/// A staged location allows creating a blob locally, uploading it to object
/// storage, and then finalizing a volume. It does not help when relocating an
/// object inside a physical blob.
///
/// Because staged locations are effectively dangling pointers, volumes cannot
/// be serialized while they're being staged.
// # TODO: should we guarantee inodes are stable in the docs?
#[derive(Debug)]
pub(crate) struct VolumeMetadata {
    version: Version,

    // the next available ino. must start at Ino::Root.add(1) for an empty
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
    #[inline]
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

pub enum Modify {
    /// Set a new ByteRange, overwriting anything already there.
    Set(ByteRange),

    /// Set the max of the current ByteRange::len and the given u64 as the new ByteRange::len.
    Max(u64),
}

#[allow(unused)]
impl VolumeMetadata {
    /// Create a new empty volume.
    pub(crate) fn empty() -> Self {
        Self::new(Version::empty())
    }

    pub(crate) fn new(version: Version) -> Self {
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
            version,
            next_ino: Ino::min_regular(),
            locations: vec![],
            data,
            dirs,
        }
    }

    const fn reserved_entries() -> [Entry; 4] {
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
                    ino: Ino::VERSION,
                    size: 0,
                    mtime: UNIX_EPOCH,
                    ctime: UNIX_EPOCH,
                    kind: FileType::Regular,
                },
                data: EntryData::Dynamic,
            },
            Entry {
                name: Cow::Borrowed(".commit"),
                parent: Ino::Root,
                attr: FileAttr {
                    ino: Ino::COMMIT,
                    size: 0,
                    mtime: UNIX_EPOCH,
                    ctime: UNIX_EPOCH,
                    kind: FileType::Regular,
                },
                data: EntryData::Dynamic,
            },
            Entry {
                name: Cow::Borrowed(".clearcache"),
                parent: Ino::Root,
                attr: FileAttr {
                    ino: Ino::CLEAR_CACHE,
                    size: 0,
                    mtime: UNIX_EPOCH,
                    ctime: UNIX_EPOCH,
                    kind: FileType::Regular,
                },
                data: EntryData::Dynamic,
            },
        ]
    }

    /// Returns the current version of the volume.
    pub(crate) fn version(&self) -> &Version {
        &self.version
    }

    pub(crate) fn set_version(&mut self, next_version: Version) {
        self.version = next_version;
    }

    /// Check whether this volume is being staged. Staged volumes contain
    /// unstable data references and can't be serialized.
    pub(crate) fn is_staged(&self) -> bool {
        self.locations
            .iter()
            .any(|l| matches!(l, Location::Staged { .. }))
    }

    /// Create a new directory.
    ///
    /// Returns an error if the parent directory does not exist, or if it
    /// already contains a file or directory with that name.
    pub(crate) fn mkdir(&mut self, parent: Ino, name: String) -> crate::Result<&FileAttr> {
        // validate that it's okay to create the directory before any state is
        // modified - don't want to undo anything if we can help it
        //
        // lookup checks parent is a directory.
        let _ = self.dir_entry(parent)?;
        if self.lookup_unchecked(parent, &name).is_some() {
            return Err(ErrorKind::AlreadyExists.into());
        }

        // start modifying things
        let ino = self.next_ino()?;
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
    pub(crate) fn mkdir_all(
        &mut self,
        parent: Ino,
        dir_names: impl IntoIterator<Item = String>,
    ) -> crate::Result<&FileAttr> {
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
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                    self.lookup_unchecked(dir, &dir_name)
                        .expect("BUG: lookup failed after exists")
                        .ino
                }
                Err(e) => return Err(e),
            }
        }

        // you gotta pass something man
        if depth == 0 {
            return Err(Error::new(ErrorKind::InvalidData, "empty path"));
        }

        Ok(self
            .data
            .get(&dir)
            .map(|e| &e.attr)
            .expect("BUG: lookup failed after mkdir"))
    }

    /// Remove a directory.
    ///
    /// Removal will fail if the directory is not empty.
    pub(crate) fn rmdir(&mut self, parent: Ino, name: &str) -> crate::Result<()> {
        let _ = self.dir_entry(parent)?;

        // TODO: we shouldn't have to copy the string here, but
        // invariance/subtyping of mut references somehow mean that the key
        // being EntryKey<'static> implies that this needs to be owned.
        let k = EntryKey::from((parent, name.to_string()));

        let ino = *self.dirs.get(&k).ok_or(ErrorKind::NotFound)?;
        let data_entry = match self.data.entry(ino) {
            btree_map::Entry::Vacant(_) => unreachable!("BUG: inconsistent dir entry"),
            btree_map::Entry::Occupied(entry) => entry,
        };

        match data_entry.get().data {
            EntryData::Dynamic | EntryData::File { .. } => Err(ErrorKind::NotADirectory.into()),
            EntryData::Directory => {
                let not_empty = self.dirs.range(entry_range(ino)?).any(|_| true);
                if not_empty {
                    return Err(ErrorKind::DirectoryNotEmpty.into());
                }

                data_entry.remove();
                self.dirs.remove(&k);
                Ok(())
            }
        }
    }

    /// Rename a file or directory.
    ///
    /// Rename will fail if the target exists but is of a different type. Rename will also
    /// fail if the target is a non-empty directory.
    pub(crate) fn rename(
        &mut self,
        parent: Ino,
        name: &str,
        newparent: Ino,
        newname: String,
    ) -> crate::Result<()> {
        let src = match self.lookup(parent, name)? {
            Some(e) => e,
            None => return Err(ErrorKind::NotFound.into()),
        };
        match self.lookup(newparent, &newname)? {
            // renames only work if the two targets are of the same FileType
            Some(dst) if dst.kind != src.kind => {
                let kind = match dst.kind {
                    FileType::Regular => ErrorKind::NotADirectory,
                    FileType::Directory => ErrorKind::IsADirectory,
                };
                return Err(Error::new(
                    kind,
                    format!(
                        "src and dst must be the same file type: {:?} -> {:?}",
                        src.kind, dst.kind
                    ),
                ));
            }
            // not allowed to rename and overwrite a non-empty directory
            Some(e) if e.kind == FileType::Directory => {
                let not_empty = self.dirs.range(entry_range(e.ino)?).any(|_| true);
                if not_empty {
                    return Err(ErrorKind::DirectoryNotEmpty.into());
                }
            }
            _ => (), // ok!
        }

        // insert it under the new parent, then remove it from the old parent dir-entry
        self.dirs.insert((newparent, newname).into(), src.ino);
        // TODO: we shouldn't have to copy the string here, but
        // invariance/subtyping of mut references somehow mean that the key
        // being EntryKey<'static> implies that this needs to be owned.
        let key = EntryKey::from((parent, name.to_string()));
        self.dirs.remove(&key);

        Ok(())
    }

    /// Create a file.
    ///
    /// If `exclusive` is set and the file already exists, creation will fail
    /// otherwise any existing file will be overwritten. Creation will always
    /// fail if the parent directory does not exist or a directory with the same
    /// name already exists.
    pub(crate) fn create(
        &mut self,
        parent: Ino,
        name: String,
        exclusive: bool,
        location: Location,
        byte_range: ByteRange,
    ) -> crate::Result<&FileAttr> {
        // validate that the parent directory exists and we're allowed to
        // create this file (permissions, O_EXCL, etc) before modifying
        // any state
        let _ = self.dir_entry(parent)?;
        match self.lookup_unchecked(parent, &name) {
            Some(e) if exclusive => return Err(ErrorKind::AlreadyExists.into()),
            Some(e) if e.is_directory() => return Err(ErrorKind::IsADirectory.into()),
            _ => (), // ok!
        }

        // verification is okay, start modifying things
        let ino = self.next_ino()?;
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

    /// Remove a file from a volume.
    pub(crate) fn delete(&mut self, parent: Ino, name: &str) -> crate::Result<()> {
        let _ = self.dir_entry(parent)?;

        // TODO: we shouldn't have to copy the string here, but
        // invariance/subtyping of mut references somehow mean that the key
        // being EntryKey<'static> implies that this needs to be owned.
        let k = EntryKey::from((parent, name.to_string()));

        let dir_entry = match self.dirs.entry(k) {
            btree_map::Entry::Vacant(_) => return Err(ErrorKind::NotFound.into()),
            btree_map::Entry::Occupied(entry) => entry,
        };
        let data_entry = match self.data.entry(*dir_entry.get()) {
            btree_map::Entry::Vacant(_) => unreachable!("BUG: inconsistent dir entry"),
            btree_map::Entry::Occupied(entry) => entry,
        };

        match data_entry.get().data {
            EntryData::File { .. } => {
                dir_entry.remove();
                data_entry.remove();
                Ok(())
            }
            EntryData::Directory => Err(ErrorKind::IsADirectory.into()),
            EntryData::Dynamic => Err(ErrorKind::PermissionDenied.into()),
        }
    }

    fn next_ino(&mut self) -> crate::Result<Ino> {
        let ino = self.next_ino;
        self.next_ino = self.next_ino.add(1)?;
        Ok(ino)
    }

    /// Lookup a directory entry by name.
    pub(crate) fn lookup(&self, parent: Ino, name: &str) -> crate::Result<Option<&FileAttr>> {
        let _ = self.dir_entry(parent)?;

        Ok(self.lookup_unchecked(parent, name))
    }

    /// Do a lookup without checking if `parent` is a directory.
    ///
    /// This is not unsafe, but is unchecked in that it won't return
    /// an error if the parent doesn't exist - it will just return None.
    // TODO: should this return Entry? we could remove a call or two in
    // a few higher level fns
    #[inline]
    fn lookup_unchecked(&self, parent: Ino, name: &str) -> Option<&FileAttr> {
        let k = EntryKey::from((parent, name));

        let ino = self.dirs.get(&k)?;
        self.data.get(ino).map(|dent| &dent.attr)
    }

    #[inline]
    fn lookup_unchecked_mut(&mut self, parent: Ino, name: &str) -> Option<&mut Entry> {
        let k = EntryKey::from((parent, name));

        let ino = self.dirs.get(&k)?;
        self.data.get_mut(ino)
    }

    /// Obtain information about a file based only on its `ino`.
    pub(crate) fn getattr(&self, ino: Ino) -> Option<&FileAttr> {
        self.data.get(&ino).map(|dent| &dent.attr)
    }

    pub(crate) fn setattr(
        &mut self,
        ino: Ino,
        mtime: Option<SystemTime>,
        ctime: Option<SystemTime>,
    ) -> crate::Result<&FileAttr> {
        let entry = self.data.get_mut(&ino).ok_or(ErrorKind::NotFound)?;
        if let Some(mtime) = mtime {
            entry.attr.mtime = mtime;
        }
        if let Some(ctime) = ctime {
            entry.attr.ctime = ctime;
        }
        Ok(&entry.attr)
    }

    /// Change the physical location of a data blob.
    ///
    /// This changes the physical location for all files in the volume that
    /// refer to this blob. To relocate an individual file, see `modify`.
    pub(crate) fn relocate(&mut self, from: &Location, to: Location) -> crate::Result<()> {
        let Some(from) = self.locations.iter_mut().find(|l| l == &from) else {
            return Err(ErrorKind::NotFound.into());
        };
        *from = to;
        Ok(())
    }

    /// Clean up all staged locations, replacing all internal state to instead point to dest.
    ///
    /// This attempts to keep Volume in a consistent state at every step of modification.
    pub(crate) fn clean_staged_locations(&mut self, dest: Location) {
        // find the first staged location within self.locations and use that as the final spot
        // for dest.
        let Some((idx, location)) = self
            .locations
            .iter_mut()
            .enumerate()
            .find(|(i, l)| l.is_staged())
        else {
            return;
        };
        *location = dest.clone();

        // we're making an assumption here that once you see the first staged location,
        // every location after that must also be staged. so if we see a location_idx > idx, this
        // means it's a staged location.
        assert!(
            &self
                .locations
                .iter()
                .skip(idx + 1)
                .all(|l| l.is_staged() || l == &dest)
        );

        // update all relevant EntryData::File::location_idx to point to the same idx we found
        // above!
        for (_, Entry { data, .. }) in self.data.iter_mut() {
            let EntryData::File { location_idx, .. } = data else {
                continue;
            };
            *location_idx = idx.min(*location_idx);
        }

        // lop off the tail
        self.locations.truncate(idx + 1);
    }

    /// Update a file's metadata to reflect that a file's data has been modified.
    ///
    /// This method changes the location of a file, it's range of bytes within that
    /// location, or both.
    pub(crate) fn modify(
        &mut self,
        ino: Ino,
        location: Option<Location>,
        range: Option<Modify>,
    ) -> crate::Result<()> {
        let Some(entry) = self.data.get_mut(&ino) else {
            return Err(ErrorKind::NotFound.into());
        };
        let EntryData::File {
            location_idx,
            byte_range,
        } = &mut entry.data
        else {
            return Err(ErrorKind::IsADirectory.into());
        };

        if let Some(location) = location {
            let new_location = insert_unique(&mut self.locations, location);
            *location_idx = new_location;
        }
        match range {
            Some(Modify::Set(range)) => {
                *byte_range = range;
                entry.attr.size = byte_range.len;
            }
            Some(Modify::Max(len)) => {
                byte_range.len = std::cmp::max(byte_range.len, len);
                entry.attr.size = byte_range.len;
            }
            None => (),
        }

        Ok(())
    }

    /// Iterate over the entires in a directory. Returns an iterator of
    /// `(filename, attr)` pairs.
    ///
    /// Iterator order is not guaranteed to be stable.
    pub(crate) fn readdir<'a>(&'a self, ino: Ino) -> crate::Result<ReadDir<'a>> {
        let _ = self.dir_entry(ino)?;

        Ok(ReadDir {
            range: self.dirs.range(entry_range(ino)?),
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
    pub(crate) fn walk<'a>(&'a self, ino: Ino) -> crate::Result<WalkIter<'a>> {
        let root_dir = self.dir_entry(ino)?;
        let root_iter = ReadDir {
            range: self.dirs.range(entry_range(ino)?),
            data: &self.data,
        };
        Ok(WalkIter {
            volume: self,
            readdirs: vec![root_iter],
            ancestors: Vec::new(),
        })
    }

    /// Iterate through all staged files within the Volume. Iteration is done in ascending Ino
    /// order.
    ///
    /// Returns an iterator over `(FileAttr, PathBuf)` tuples.
    pub(crate) fn iter_staged(&self) -> impl Iterator<Item = (&FileAttr, &PathBuf)> {
        self.data
            .iter()
            .filter_map(|(ino, entry)| match self.location(*ino) {
                Some((Location::Staged { path }, _)) => Some((&entry.attr, path)),
                _ => None,
            })
    }

    #[inline]
    fn dir_entry(&self, ino: Ino) -> crate::Result<&Entry> {
        let entry = self.data.get(&ino).ok_or(ErrorKind::NotFound)?;
        if !entry.is_dir() {
            return Err(ErrorKind::NotADirectory.into());
        }
        Ok(entry)
    }

    /// Get a file's physical location for opening and reading.
    ///
    /// Attempting to get the physical location of a directory or a symlink
    /// returns an error.
    pub(crate) fn location(&self, ino: Ino) -> Option<(&Location, &ByteRange)> {
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

    /// Serialize committed data in this volume to bytes.
    ///
    /// Note: uncommitted changes will be dropped. Commit if you want them persisted!
    pub(crate) fn to_bytes(&self) -> crate::Result<Vec<u8>> {
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
                dir_entries.push(to_fb_entry(&mut fbb, entry)?);
            }
            fbb.create_vector(&dir_entries)
        };

        let fb_version = fbb.create_string(self.version.as_ref());
        let volume = fb::Volume::create(
            &mut fbb,
            &fb::VolumeArgs {
                version: Some(fb_version),
                locations: Some(locations),
                entries: Some(entries),
            },
        );
        fbb.finish(volume, None);
        Ok(fbb.finished_data().into())
    }

    /// Read a serialized volume. Returns an error if the volume is invalid or
    /// inconsistent.
    pub(crate) fn from_bytes(bs: &[u8]) -> crate::Result<Self> {
        let fb_volume = fb::root_as_volume(bs).map_err(|e| {
            Error::with_source(ErrorKind::InvalidData, "failed to parse volume metadata", e)
        })?;

        let locations: Vec<_> = fb_volume.locations().iter().map(from_fb_location).collect();

        let mut max_ino = Ino::min_regular();
        let version = Version::from_str(fb_volume.version())?;
        let mut volume = Self::new(version);

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
        volume.next_ino = max_ino.add(1)?;

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

fn entry_range(ino: Ino) -> crate::Result<std::ops::Range<EntryKey<'static>>> {
    let start: EntryKey = (ino, "").into();
    let end: EntryKey = (ino.add(1)?, "").into();
    Ok(start..end)
}

/// The iterator returned from [readdir][Volume::readdir].
pub(crate) struct ReadDir<'a> {
    data: &'a BTreeMap<Ino, Entry>,
    range: btree_map::Range<'a, EntryKey<'static>, Ino>,
}

impl<'a> Iterator for ReadDir<'a> {
    type Item = (&'a str, &'a FileAttr);

    fn next(&mut self) -> Option<Self::Item> {
        self.range.next().map(|(EntryKey(k), ino)| {
            let dent = self
                .data
                .get(ino)
                .unwrap_or_else(|| panic!("BUG: invalid dirent: ino={ino:?}"));
            (k.name.as_ref(), &dent.attr)
        })
    }
}

/// The iterator returned from [walk][Volume::walk].
pub(crate) struct WalkIter<'a> {
    volume: &'a VolumeMetadata,

    // a stack of entry iterators
    readdirs: Vec<ReadDir<'a>>,

    // the names of all the directories opened to get here
    ancestors: Vec<&'a str>,
}

impl<'a> Iterator for WalkIter<'a> {
    // TODO: it's a big gnarly to be cloning and returning the ancestors path
    // every time but the lifetime on returning a slice referencing self
    // is a pain to express.
    type Item = crate::Result<(&'a str, Vec<&'a str>, &'a FileAttr)>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.readdirs.is_empty() {
            let next = self.readdirs.last_mut()?.next();
            match next {
                Some((name, attr)) => {
                    let ancestors = if attr.is_directory() {
                        let next = match self.volume.readdir(attr.ino) {
                            Ok(next) => next,
                            Err(e) => return Some(Err(e)),
                        };
                        let ancestors = self.ancestors.clone();
                        self.readdirs.push(next);
                        self.ancestors.push(name);
                        ancestors
                    } else {
                        self.ancestors.clone()
                    };
                    return Some(Ok((name, ancestors, attr)));
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

fn to_fb_entry<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    entry: &Entry,
) -> crate::Result<WIPOffset<fb::Entry<'a>>> {
    let attrs = fb::FileAttrs::create(
        fbb,
        &fb::FileAttrsArgs {
            ino: entry.attr.ino.into(),
            size: entry.attr.size,
            mtime: Some(&entry.attr.mtime.try_into()?),
            ctime: Some(&entry.attr.ctime.try_into()?),
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
    Ok(fb::Entry::create(
        fbb,
        &fb::EntryArgs {
            name: Some(name),
            parent_ino: entry.parent.into(),
            attrs: Some(attrs),
            location_ref,
        },
    ))
}

fn from_fb_entry(fb_entry: &fb::Entry) -> crate::Result<Entry> {
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
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "missing file data pointer",
                ));
            };
            let Some(byte_range) = location_ref.byte_range() else {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "missing file data range",
                ));
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
    };

    Ok(Entry {
        name,
        attr,
        parent: parent_ino.into(),
        data,
    })
}

impl TryFrom<SystemTime> for fb::Timespec {
    type Error = crate::Error;

    fn try_from(time: SystemTime) -> Result<Self, Self::Error> {
        let since_epoch = time.duration_since(UNIX_EPOCH).map_err(|e| {
            Error::with_source(
                ErrorKind::InvalidData,
                format!("bad timestamp: {:?}", time),
                e,
            )
        })?;
        Ok(fb::Timespec::new(
            since_epoch.as_secs(),
            since_epoch.subsec_nanos(),
        ))
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
        }
    }
}

impl TryFrom<fb::FileType> for FileType {
    type Error = Error;

    fn try_from(ft: fb::FileType) -> Result<Self, Self::Error> {
        match ft {
            fb::FileType::Regular => Ok(FileType::Regular),
            fb::FileType::Directory => Ok(FileType::Directory),
            ft => Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "unknown file type: code={code}, name={name}",
                    code = ft.0,
                    name = ft.variant_name().unwrap_or("")
                ),
            )),
        }
    }
}

fn to_fb_location<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    location: &Location,
) -> crate::Result<WIPOffset<fb::Location<'a>>> {
    match location {
        Location::Committed { key } => {
            let key = fbb.create_string(key.as_ref().as_ref());
            Ok(fb::Location::create(
                fbb,
                &fb::LocationArgs { key: Some(key) },
            ))
        }
        _ => Err(Error::new(
            ErrorKind::InvalidData,
            "can't serialize a volume with staged data",
        )),
    }
}

fn from_fb_location(fb_location: fb::Location) -> Location {
    Location::committed(fb_location.key())
}

#[cfg(test)]
mod test {
    use crate::error::ErrorKind;

    use super::*;

    fn readdir_nospecial(
        v: &VolumeMetadata,
        ino: Ino,
    ) -> crate::Result<impl Iterator<Item = (&str, &FileAttr)>> {
        let _ = v.dir_entry(ino)?;

        let iter = ReadDir {
            range: v.dirs.range(entry_range(ino)?),
            data: &v.data,
        }
        .filter(|(_, attr)| attr.ino.is_regular());

        Ok(iter)
    }

    #[test]
    fn open() {
        let mut volume = VolumeMetadata::empty();
        let f1 = volume
            .create(
                Ino::Root,
                "zzzz".to_string(),
                true,
                Location::committed("zzzz"),
                ByteRange {
                    offset: 0,
                    len: 123,
                },
            )
            .unwrap()
            .clone();
        // location should match what we just created with
        let (l1, _) = volume.location(f1.ino).unwrap();
        assert_eq!(l1, &Location::committed("zzzz"));

        let f2 = volume
            .create(
                Ino::Root,
                "aaaa".to_string(),
                true,
                Location::committed("aaaa"),
                ByteRange {
                    offset: 0,
                    len: 123,
                },
            )
            .unwrap()
            .clone();

        // old locations should be stable
        let (l1, _) = volume.location(f1.ino).unwrap();
        assert_eq!(l1, &Location::committed("zzzz"));
        // location should match what we just created with
        let (l2, _) = volume.location(f2.ino).unwrap();
        assert_eq!(l2, &Location::committed("aaaa"));
    }

    #[test]
    fn delete() {
        let mut volume = VolumeMetadata::empty();
        let mk_file = |volume: &mut VolumeMetadata, parent| {
            volume
                .create(
                    parent,
                    "zzzz".to_string(),
                    true,
                    Location::committed("zzzz"),
                    ByteRange {
                        offset: 0,
                        len: 123,
                    },
                )
                .unwrap()
                .clone()
        };

        let a = volume.mkdir(Ino::Root, "a".to_string()).unwrap().clone();
        let f1 = mk_file(&mut volume, Ino::Root);
        let f2 = mk_file(&mut volume, a.ino);

        // location should match what we just created with
        let (l1, _) = volume.location(f1.ino).unwrap();
        assert_eq!(l1, &Location::committed("zzzz"));
        let (l1, _) = volume.location(f2.ino).unwrap();
        assert_eq!(l1, &Location::committed("zzzz"));

        // delete the first file
        volume.delete(Ino::Root, "zzzz").unwrap();
        assert_eq!(volume.location(f1.ino), None);
        let (l1, _) = volume.location(f2.ino).unwrap();
        assert_eq!(l1, &Location::committed("zzzz"));

        // delete both files
        volume.delete(a.ino, "zzzz").unwrap();
        assert_eq!(volume.location(f1.ino), None);
        assert_eq!(volume.location(f2.ino), None);

        // should just contain the directory, not the initial file
        let names: Vec<_> = readdir_nospecial(&volume, Ino::Root)
            .unwrap()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(vec!["a"], names);
        // should be empty
        let names: Vec<_> = volume
            .readdir(a.ino)
            .unwrap()
            .map(|(name, _)| name)
            .collect();
        assert!(names.is_empty());
    }

    #[test]
    fn delete_directory() {
        let mut volume = VolumeMetadata::empty();
        let _ = volume.mkdir(Ino::Root, "a".to_string()).unwrap();
        assert_eq!(
            volume.delete(Ino::Root, "a").map_err(|e| e.kind()),
            Err(ErrorKind::IsADirectory)
        );
    }

    #[test]
    fn delete_special_file() {
        let mut volume = VolumeMetadata::empty();

        for entry in VolumeMetadata::reserved_entries() {
            if entry.is_dir() {
                continue;
            }
            assert_eq!(
                volume.delete(Ino::Root, &entry.name).map_err(|e| e.kind()),
                Err(ErrorKind::PermissionDenied)
            );
        }
    }

    #[test]
    fn lookup() {
        let mut volume = VolumeMetadata::empty();
        let a = volume.mkdir(Ino::Root, "a".to_string()).unwrap().clone();
        let b = volume.mkdir(a.ino, "b".to_string()).unwrap().clone();
        let c = volume.mkdir(b.ino, "c".to_string()).unwrap().clone();

        volume
            .create(
                c.ino,
                "test.txt".to_string(),
                true,
                Location::committed("test-key.txt"),
                ByteRange { offset: 0, len: 64 },
            )
            .unwrap();

        // the root directory has some special files. ignore them.
        assert!(
            volume
                .readdir(Ino::Root)
                .unwrap()
                .any(|(name, attr)| name == "a" && attr.is_directory())
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
        let mut volume = VolumeMetadata::empty();
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
    fn rename() {
        let mut volume = VolumeMetadata::empty();

        volume
            .mkdir_all(Ino::Root, ["dir".to_string(), "a".to_string()])
            .unwrap();

        volume
            .create(
                Ino::Root,
                "file".to_string(),
                true,
                Location::committed("test-key.txt"),
                ByteRange { offset: 0, len: 64 },
            )
            .unwrap();

        // can't rename to inodes of different types
        assert_eq!(
            volume
                .rename(Ino::Root, "dir", Ino::Root, "file".to_string())
                .unwrap_err()
                .kind(),
            ErrorKind::NotADirectory
        );
        assert_eq!(
            volume
                .rename(Ino::Root, "file", Ino::Root, "dir".to_string())
                .unwrap_err()
                .kind(),
            ErrorKind::IsADirectory
        );

        // we can't rename a dir to a non-empty dir
        volume
            .mkdir_all(Ino::Root, ["dir2".to_string(), "a".to_string()])
            .unwrap();
        assert_eq!(
            volume
                .rename(Ino::Root, "dir", Ino::Root, "dir2".to_string())
                .unwrap_err()
                .kind(),
            ErrorKind::DirectoryNotEmpty
        );

        // you can rename to a non-empty file though
        volume
            .create(
                Ino::Root,
                "file2".to_string(),
                true,
                Location::committed("test-key.txt"),
                ByteRange { offset: 0, len: 64 },
            )
            .unwrap();
        assert!(
            volume
                .rename(Ino::Root, "file", Ino::Root, "file2".to_string())
                .is_ok()
        );

        // and you can rename to an existing but empty dir
        volume.mkdir(Ino::Root, "dir3".to_string()).unwrap();
        assert!(
            volume
                .rename(Ino::Root, "dir", Ino::Root, "dir3".to_string())
                .is_ok()
        );
    }

    #[test]
    fn walk() {
        fn all_paths(volume: &VolumeMetadata) -> Vec<String> {
            volume
                .walk(Ino::Root)
                .unwrap()
                .filter_map(|entry| {
                    let (name, mut dirs, attr) = entry.unwrap();
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

        let mut volume = VolumeMetadata::empty();
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
                Location::committed("test-key.txt"),
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
                Location::committed("test-key.txt"),
                (0, 10).into(),
            )
            .unwrap();

        assert_eq!(
            all_paths(&volume),
            vec!["a", "a/b", "a/b/c.txt", "a/c", "a/c/e", "a/c/e/f.txt"],
        )
    }

    fn assert_lookup(v: &VolumeMetadata, ino: Ino, name: &str) -> FileAttr {
        v.lookup(ino, name).unwrap().unwrap().clone()
    }

    fn assert_read_test_txt(volume: &VolumeMetadata) {
        let a = assert_lookup(volume, Ino::Root, "a");
        let b = assert_lookup(volume, a.ino, "b");
        let c = assert_lookup(volume, b.ino, "c");
        let test_txt = assert_lookup(volume, c.ino, "test.txt");

        assert_eq!(
            volume.location(test_txt.ino).unwrap(),
            (
                &Location::committed("test-key.txt"),
                &ByteRange { offset: 0, len: 64 },
            )
        );
    }

    #[test]
    fn modify_set() {
        let mut meta = VolumeMetadata::empty();

        let ino = meta
            .create(
                Ino::Root,
                "file".to_string(),
                false,
                Location::committed("old"),
                (0, 3).into(),
            )
            .unwrap()
            .ino;

        let new_location = Location::committed("new");
        let new_range = ByteRange { offset: 10, len: 8 };

        meta.modify(
            ino,
            Some(new_location.clone()),
            Some(Modify::Set(new_range)),
        )
        .unwrap();

        let (location, range) = meta.location(ino).unwrap();
        assert_eq!(location, &new_location);
        assert_eq!(*range, new_range);

        let attr = meta.getattr(ino).unwrap();
        assert_eq!(attr.size, new_range.len);
    }

    #[test]
    fn modify_max() {
        let mut meta = VolumeMetadata::empty();
        let ino = meta
            .create(
                Ino::Root,
                "grow".to_string(),
                false,
                Location::staged("grow"),
                ByteRange::empty(),
            )
            .unwrap()
            .ino;

        meta.modify(ino, None, Some(Modify::Max(8))).unwrap();
        let (location, range) = meta.location(ino).unwrap();
        assert!(matches!(location, Location::Staged { .. }));
        assert_eq!(range.len, 8);

        // no-op since it's a smaller len
        meta.modify(ino, None, Some(Modify::Max(3))).unwrap();
        let attr = meta.getattr(ino).unwrap();
        assert_eq!(attr.size, 8);
        let (_, range_after) = meta.location(ino).unwrap();
        assert_eq!(range_after.len, 8);
    }
}
