use flatbuffers::InvalidFlatbuffer;

#[path = "../dirent.fbs.rs"]
#[allow(warnings)]
#[rustfmt::skip]
mod dirent_fbs;
pub use dirent_fbs::{
    DirEntry, DirEntryArgs, DirEntryPage, DirEntryPageArgs, FileType, Timespec,
    root_as_dir_entry_page,
};

/// Wrapper around the buffer containing the FlatBuffer-encoded `DirEntryPage`. Provides zero-copy
/// access to the individual `DirEntry`s within the Page.
pub(crate) struct DirEntryPageOwned {
    /// the flatbuffer
    buf: Vec<u8>,
}

#[allow(dead_code)]
impl DirEntryPageOwned {
    // ~4MiB maximum. 8192 dirents per page, with each dirent being ~500 bytes (assuming a 255char filename).
    pub const DIRENTS_PER_PAGE: usize = 1 << 14;

    pub(crate) fn new(buf: Vec<u8>) -> Result<Self, InvalidFlatbuffer> {
        // root_as_dir_entry_page does validation of the flatbuffer
        let _ = root_as_dir_entry_page(&buf)?;
        Ok(Self { buf })
    }

    /// Accessor for the page which maps inode to the page it belongs to.
    #[inline]
    fn page(&self) -> DirEntryPage<'_> {
        // root_as_dir_entry_page has validation overhead, but we've already validated in the ctor
        // so we don't need to take the hit for that each time. this is zero overhead.
        // this is UDB if the buffer was corrupted afterwards so beware of cosmic rays?
        unsafe { flatbuffers::root_unchecked::<DirEntryPage>(&self.buf) }
    }

    /// Get the DirEntry of the given inode.
    #[inline]
    pub(crate) fn dirent(&self, ino: u64) -> Option<DirEntry<'_>> {
        // lookup_by_key does a binary-search to find the page the inode belongs to
        // this assumes that the dirents are sorted by ino
        self.page().dirents().lookup_by_key(ino, cmp_ino)
    }

    /// Destroy the DirEntryPageOwned and return the underlying buffer.
    pub(crate) fn into_flatbuffer(self) -> Vec<u8> {
        self.buf
    }
}

fn cmp_ino(dirent: &DirEntry, ino: &u64) -> std::cmp::Ordering {
    dirent.ino().cmp(ino)
}
