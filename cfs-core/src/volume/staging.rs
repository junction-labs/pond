use std::{
    collections::BTreeMap,
    hash::{DefaultHasher, Hash, Hasher},
    os::unix::fs::MetadataExt,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::volume::{
    Header, HeaderArgs, HeaderOwned,
    dirent::{
        DirEntry, DirEntryArgs, DirEntryPage, DirEntryPageArgs, DirEntryPageOwned, FileType,
        Timespec,
    },
    dirent_index::{Entry, Index, IndexArgs, Page},
};

#[allow(dead_code)]
#[derive(Clone, PartialEq)]
pub(crate) enum FileLocation {
    Local(PathBuf),
    S3 {
        bucket: String,
        key: String,
        /// offset into the S3 object where the file starts
        offset: u64,
        /// size of the file in bytes
        size: u64,
    },
}

#[allow(dead_code)]
pub(crate) struct FileMetadata {
    pub(crate) parent_ino: u64,
    pub(crate) name: String,
    pub(crate) attr: fuser::FileAttr,
}

#[allow(dead_code)]
pub(crate) enum StagedFile {
    Regular {
        metadata: FileMetadata,
        source: FileLocation,
    },
    Directory(FileMetadata),
}

#[allow(dead_code)]
impl StagedFile {
    fn to_flatbuffer<'a>(
        &self,
        fbb: &mut FlatBufferBuilder<'a>,
        file_offsets: &BTreeMap<u64, (u64, u64)>,
    ) -> WIPOffset<DirEntry<'a>> {
        let metadata = match self {
            StagedFile::Regular { metadata, .. } => metadata,
            StagedFile::Directory(metadata) => metadata,
        };
        let (s3_offset, s3_size) = file_offsets
            .get(&metadata.attr.ino)
            .copied()
            .unwrap_or((0, 0)); // i.e. it's not a File
        let args = DirEntryArgs {
            ino: metadata.attr.ino,
            parent_ino: metadata.parent_ino,
            size: metadata.attr.size,
            blocks: metadata.attr.blocks,
            atime: Some(&metadata.attr.atime.into()),
            mtime: Some(&metadata.attr.mtime.into()),
            ctime: Some(&metadata.attr.ctime.into()),
            kind: metadata.attr.kind.into(),
            perm: metadata.attr.perm,
            nlink: metadata.attr.nlink,
            uid: metadata.attr.uid,
            gid: metadata.attr.gid,
            rdev: metadata.attr.rdev,
            name: Some(fbb.create_string(&metadata.name)),
            s3_offset,
            s3_size,
        };

        DirEntry::create(fbb, &args)
    }
}

impl From<fuser::FileType> for FileType {
    fn from(filetype: fuser::FileType) -> Self {
        match filetype {
            fuser::FileType::RegularFile => FileType::Regular,
            fuser::FileType::Directory => FileType::Directory,
            fuser::FileType::Symlink => FileType::Symlink,
            _ => panic!(),
        }
    }
}

impl From<SystemTime> for Timespec {
    fn from(time: SystemTime) -> Self {
        let since_epoch = time
            .duration_since(UNIX_EPOCH)
            .expect("time should not go backwards");
        Timespec::new(since_epoch.as_secs(), since_epoch.subsec_nanos())
    }
}

#[allow(dead_code)]
pub(crate) struct Staging {
    pub(crate) dirents: BTreeMap<u64, StagedFile>,
}

// TODO: you should be able to ls/read from Staging too. The idea is that Staging is looked at
// before the actual Volume, since Staging is basically a new version on top of the Volume.
#[allow(dead_code)]
impl Staging {
    pub(crate) fn mkdir(&mut self, parent_ino: u64, dirname: String) {
        let ino = ino(parent_ino, &dirname);
        let now = SystemTime::now();
        let attr = fuser::FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: UNIX_EPOCH, // macOS only
            kind: fuser::FileType::Directory,
            perm: 0o777, // ¯\_(ツ)_/¯
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 0, // not applicable?
            flags: 0,   // macOS only
        };
        self.dirents.insert(
            ino,
            StagedFile::Directory(FileMetadata {
                parent_ino,
                name: dirname,
                attr,
            }),
        );
    }

    pub(crate) fn add_file(
        &mut self,
        parent_ino: u64,
        filename: String,
        path: PathBuf,
    ) -> std::io::Result<()> {
        let metadata = std::fs::metadata(&path)?;
        if !metadata.is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "add_file only accepts files",
            ));
        }

        let ino = ino(parent_ino, &filename);
        let now = SystemTime::now();
        let attr = fuser::FileAttr {
            ino,
            size: metadata.size(),
            blocks: metadata.blocks(),
            atime: now,
            mtime: now,
            ctime: now,
            crtime: UNIX_EPOCH, // macOS only
            kind: fuser::FileType::RegularFile,
            perm: 0o777, // ¯\_(ツ)_/¯
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 0, // not applicable?
            flags: 0,   // macOS only
        };
        self.dirents.insert(
            ino,
            StagedFile::Regular {
                metadata: FileMetadata {
                    parent_ino,
                    name: filename,
                    attr,
                },
                source: FileLocation::Local(path),
            },
        );

        Ok(())
    }

    pub(crate) fn into_flatbuffer(self) -> Vec<u8> {
        // dirents are sorted by parent_ino before being split into pages
        let mut dirents: Vec<_> = self.dirents.into_iter().collect();
        dirents.sort_by_key(|(_, file)| match file {
            StagedFile::Regular { metadata, .. } => metadata.parent_ino,
            StagedFile::Directory(metadata) => metadata.parent_ino,
        });

        // chunk dirents up into pages and sort each dirent within a page by its ino for faster
        // lookups.
        // NOTE: chunks_mut returns views into the original dirents vector above! it doesn't make a
        // copy.
        #[allow(clippy::manual_inspect)]
        let pages: Vec<_> = dirents
            .chunks_mut(DirEntryPageOwned::DIRENTS_PER_PAGE) // each page has
            .map(|page| {
                page.sort_by_key(|(ino, _)| *ino);
                page
            })
            .collect();

        // construct the page section, encoding all of the dirents into DirEntryPage flatbuffers.
        let mut page_offset = 0;
        let mut page_bufs = Vec::new();
        let mut page_offsets = BTreeMap::new();
        let mut ino_to_pageno = BTreeMap::new();
        // track the file offsets and files for each dirent
        let mut file_cursor = 0;
        let mut file_offsets = BTreeMap::new();
        let mut files = Vec::new();
        for (pageno, page) in pages.into_iter().enumerate() {
            // each page is a root_type
            let mut fbb = FlatBufferBuilder::new();

            let mut dirent_vec = Vec::new();
            for (ino, stagedfile) in page {
                if let StagedFile::Regular { metadata, source } = stagedfile {
                    file_offsets.insert(metadata.attr.ino, (file_cursor, metadata.attr.size));
                    file_cursor += metadata.attr.size;
                    files.push(source.clone());
                }

                ino_to_pageno.insert(*ino, pageno);
                dirent_vec.push(stagedfile.to_flatbuffer(&mut fbb, &file_offsets));
            }
            let dirents_fbs = fbb.create_vector(&dirent_vec);

            let page_fbs = DirEntryPage::create(
                &mut fbb,
                &DirEntryPageArgs {
                    dirents: Some(dirents_fbs),
                },
            );
            fbb.finish(page_fbs, None);
            let page_buf = fbb.finished_data().to_vec();

            // track page offsets by pageno
            page_offsets.insert(pageno, (page_offset, page_buf.len()));
            page_offset += page_buf.len();

            page_bufs.push(page_buf);
        }
        // the final flatbuffer section for all the pages smushed together
        let page_section: Vec<u8> = page_bufs.into_iter().flatten().collect();

        let index_section: Vec<u8> = {
            let mut fbb = FlatBufferBuilder::new();
            let entries = fbb.create_vector_from_iter(
                ino_to_pageno
                    .into_iter()
                    .map(|(ino, pageno)| Entry::new(ino, pageno as u16)),
            );
            let pages = fbb.create_vector_from_iter(page_offsets.into_iter().map(
                |(pageno, (offset, size))| Page::new(pageno as u16, offset as u64, size as u64),
            ));
            let index = Index::create(
                &mut fbb,
                &IndexArgs {
                    entries: Some(entries),
                    pages: Some(pages),
                },
            );
            fbb.finish(index, None);
            fbb.finished_data().to_vec()
        };

        let header_section = {
            let mut fbb = FlatBufferBuilder::new();

            // TODO: what version should this be? probably something we get from the metadata
            // server
            let version = fbb.create_vector(&uuid::Uuid::nil().to_bytes_le());
            let header = Header::create(
                &mut fbb,
                &HeaderArgs {
                    version: Some(version),
                    dirent_index_offset: HeaderOwned::FIXED_SIZE as u64,
                    dirent_index_size: index_section.len() as u64,
                    dirent_pages_offset: (HeaderOwned::FIXED_SIZE + index_section.len()) as u64,
                    dirent_pages_size: page_section.len() as u64,
                },
            );
            fbb.finish(header, None);
            let mut header = fbb.finished_data().to_vec();
            header.resize(HeaderOwned::FIXED_SIZE, 0);
            header
        };

        // the files! for initial testing, let's just read it all into memory lol
        // in reality, these files will all be in separate locations in s3, and we'll be doing a
        // multi-part upload into the volume object
        let file_section: Vec<u8> = {
            let mut file_contents = Vec::with_capacity(files.len());
            for file in files {
                let contents = match file {
                    FileLocation::Local(path) => {
                        std::fs::read(path).expect("should be a real file?")
                    }
                    _ => continue,
                };
                file_contents.push(contents);
            }
            file_contents.into_iter().flatten().collect()
        };

        header_section
            .into_iter()
            .chain(index_section)
            .chain(page_section)
            .chain(file_section)
            .collect()
    }
}

/// Generate a inode number for a file given its parent_ino and its name.
///
/// There's a risk of collision since it's just one hash, we can come up with a more complicated
/// scheme later?
fn ino(parent_ino: u64, name: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    (parent_ino, &name).hash(&mut hasher);
    hasher.finish()
}
