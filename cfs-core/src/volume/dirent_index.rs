use crate::volume::dirent::{DirEntry, DirEntryPageOwned};
use flatbuffers::InvalidFlatbuffer;
use std::collections::BTreeMap;

#[path = "../dirent_index.fbs.rs"]
#[allow(warnings)]
#[rustfmt::skip]
mod dirent_index_fbs;
#[allow(unused_imports)]
pub use dirent_index_fbs::{Entry, Index, IndexArgs, Page, root_as_index, root_as_index_unchecked};

/// Index over DirEntryPages that lets you lookup dir entries via inode.
pub(crate) struct DirEntryIndex {
    /// Owned FlatBuffer-encoded Page index
    buf: Vec<u8>,
    /// page-number to the FlatBuffer-encoded page
    pages: BTreeMap<u16, DirEntryPageOwned>,
}

#[allow(dead_code)]
impl DirEntryIndex {
    /// Create an Index using the FlatBuffer-encoded index buffer.
    pub(crate) fn new(buf: Vec<u8>) -> Result<Self, InvalidFlatbuffer> {
        // validate the buffer as a FlatBuffer-encoded Index
        let _ = root_as_index(&buf)?;
        Ok(Self {
            buf,
            pages: BTreeMap::new(),
        })
    }

    /// Track a new DirEntryPage within our index.
    pub(crate) fn add_page(&mut self, pageno: u16, page: DirEntryPageOwned) {
        self.pages.insert(pageno, page);
    }

    /// Accessor for the page index which maps inode to the page it belongs to.
    #[inline]
    fn page_index(&self) -> Index<'_> {
        // root_as_index has validation overhead, but we've already validated in the ctor
        // so we don't need to take the hit for that each time. this is zero overhead.
        // this is UDB if the buffer was corrupted afterwards so beware of cosmic rays?
        unsafe { root_as_index_unchecked(&self.buf) }
    }

    /// Lookup the DirEntry of the given inode.
    #[inline]
    pub(crate) fn dirent(&self, ino: u64) -> Option<DirEntry<'_>> {
        // lookup_by_key does a binary-search to find the page the inode belongs to
        // this assumes that the dirents are sorted by ino
        let entry = self.page_index().entries().lookup_by_key(ino, cmp_ino)?;
        match self.pages.get(&entry.pageno()) {
            Some(page) => page.dirent(ino),
            None => {
                todo!("page miss, load it from s3!")
            }
        }
    }

    /// Destroy the DirEntryIndex, returning the index buffer and pages buffer.
    pub(crate) fn into_flatbuffer(self) -> (Vec<u8>, Vec<u8>) {
        let index_buf = self.buf;
        let pages_buf = self
            .pages
            .into_values()
            .flat_map(|p| p.into_flatbuffer())
            .collect();

        (index_buf, pages_buf)
    }
}

/// Comparator for our FlatBuffer-encoded Entry on its inode key.
fn cmp_ino(entry: &&Entry, ino: &u64) -> std::cmp::Ordering {
    entry.ino().cmp(ino)
}
