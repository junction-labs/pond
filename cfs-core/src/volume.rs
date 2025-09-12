use flatbuffers::InvalidFlatbuffer;
use uuid::Uuid;

mod dirent;
mod dirent_index;
mod staging;

#[path = "./volume_header.fbs.rs"]
#[allow(warnings)]
#[rustfmt::skip]
mod volume_header_fbs;
#[allow(unused_imports)]
pub use volume_header_fbs::{Header, HeaderArgs, root_as_header, root_as_header_unchecked};

use crate::volume::dirent_index::DirEntryIndex;

/// A fixed-sized header for the Volume. Contains information about volume, including the offsets
/// and sizes of the dir entry index and dir entry pages.
pub struct HeaderOwned {
    buf: Vec<u8>,
}

#[allow(unused)]
impl HeaderOwned {
    /// fixed size of the header in bytes. it will always take this much space in the volume.
    pub const FIXED_SIZE: usize = 1024;

    pub(crate) fn new(buf: Vec<u8>) -> Result<Self, InvalidFlatbuffer> {
        // validate the buffer as a FlatBuffer-encoded Header
        let _ = root_as_header(&buf)?;

        Ok(Self { buf })
    }

    pub(crate) fn header(&self) -> Header<'_> {
        unsafe { root_as_header_unchecked(&self.buf) }
    }

    /// Destroys the HeaderOwned, returning the FlatBuffer-encoded buffer.
    pub(crate) fn into_flatbuffer(mut self) -> Vec<u8> {
        debug_assert!(
            self.buf.len() <= Self::FIXED_SIZE,
            "header should not exceed 1KiB in size otherwise this will result in data loss"
        );

        self.buf.resize(Self::FIXED_SIZE, 0);
        self.buf
    }
}

#[allow(unused)]
pub struct Volume {
    /// S3 bucket the volume lives in
    s3_bucket: String,
    /// S3 key to the volume
    s3_key: String,

    /// Index over `DirEntryPage`s, allowing access to `DirEntry`s using inode.
    index: DirEntryIndex,

    // buffer that holds the FlatBuffer-encoded Header
    buf: Vec<u8>,
}

#[allow(unused)]
impl Volume {
    pub async fn mount(_volume: &str, _version: &Uuid) -> anyhow::Result<Self> {
        // TODO: talk to metadata server to get the s3 object, load up the object's header.
        // then load up the dirents index
        Ok(Self {
            s3_bucket: "bucket".to_string(),
            s3_key: "key".to_string(),
            index: DirEntryIndex::new(Vec::new())?,
            buf: Vec::new(),
        })
    }

    /// Returns a Header, an accessor on top of the underlying FlatBuffer-encoded buffer.
    #[inline]
    pub fn header(&self) -> Header<'_> {
        // root_as_header has validation overhead, but we've already validated in the ctor
        // so we don't need to take the hit for that each time. this is zero overhead.
        // this is UDB if the buffer was corrupted afterwards so beware of cosmic rays?
        unsafe { root_as_header_unchecked(&self.buf) }
    }

    /// Destroys the volume and returns a buffer that holds the volume header, the dirent index,
    /// and the dirent pages.
    pub fn into_flatbuffer(self) -> Vec<u8> {
        let header = self.buf;
        let (index, pages) = self.index.into_flatbuffer();

        // chains header + index + pages buffers
        header.into_iter().chain(index).chain(pages).collect()
    }

    pub fn from_flatbuffer(mut buf: Vec<u8>) -> anyhow::Result<Self> {
        // validate that the beginning of this buffer is a valid FlatBuffer-encoded Header, and
        // grab the offset + length of the dirent index (which is also part of this buffer)
        let (dirent_index_offset, dirent_index_size) = {
            let header = root_as_header(&buf)?;
            (
                header.dirent_index_offset() as usize,
                header.dirent_index_size() as usize,
            )
        };

        // truncates buf and copies the tail over to the returned vec
        let mut index_buf = buf.split_off(dirent_index_offset);

        if index_buf.len() < dirent_index_size {
            return Err(anyhow::anyhow!(
                "the buffer contains an incomplete index -- we didn't fetch a large enough block from s3"
            ));
        }

        // truncate it since we may have fetched more than the index
        index_buf.truncate(dirent_index_size);
        let index = DirEntryIndex::new(index_buf)?;

        Ok(Volume {
            s3_bucket: "bucket".to_string(),
            s3_key: "key".to_string(),
            index,
            buf,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::volume::dirent_index::{Entry, Index, IndexArgs, Page};

    use super::*;
    use flatbuffers::FlatBufferBuilder;

    #[test]
    fn test_volume_flatbuffer_roundtrip() {
        // building the index flatbuffer
        let mut index_fbb = FlatBufferBuilder::new();
        let entries = index_fbb.create_vector(&[Entry::new(100, 0), Entry::new(200, 1)]);
        let pages = index_fbb.create_vector(&[Page::new(0, 1024, 1024), Page::new(1, 2048, 1024)]);
        let index = Index::create(
            &mut index_fbb,
            &IndexArgs {
                entries: Some(entries),
                pages: Some(pages),
            },
        );
        index_fbb.finish(index, None);
        let index_buf = index_fbb.finished_data().to_vec();

        // building the header flatbuffer
        let mut header_fbb = FlatBufferBuilder::new();
        let version_bytes = uuid::Uuid::nil().to_bytes_le();
        let version = header_fbb.create_vector(&version_bytes);
        let header = Header::create(
            &mut header_fbb,
            &HeaderArgs {
                version: Some(version),
                // dirent index starts right after the header
                dirent_index_offset: HeaderOwned::FIXED_SIZE as u64,
                dirent_index_size: index_buf.len() as u64,
                dirent_pages_offset: (HeaderOwned::FIXED_SIZE + index_buf.len()) as u64,
                dirent_pages_size: 0,
            },
        );
        header_fbb.finish(header, None);
        let mut header_buf = header_fbb.finished_data().to_vec();
        header_buf.resize(HeaderOwned::FIXED_SIZE, 0);

        // the overall buffer we can use to reconstruct the volume, sans dirent pages and files
        let buf: Vec<_> = header_buf.into_iter().chain(index_buf).collect();

        // make sure the serialization produces the same buf
        let volume = Volume::from_flatbuffer(buf.clone()).unwrap();
        assert_eq!(volume.into_flatbuffer(), buf);
    }
}
