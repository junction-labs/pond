use bytes::Bytes;
use futures::future::{BoxFuture, Shared};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, RwLock},
};

pub(crate) type BytesFuture = Shared<BoxFuture<'static, bytes::Bytes>>;

/// Similar to std::ops::Range, but the former does not implement ordering.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ByteRange {
    /// start of range, inclusive
    pub(crate) start: u64,
    /// end of range, exclusive
    pub(crate) end: u64,
}

impl ByteRange {
    pub(crate) fn contains(&self, idx: u64) -> bool {
        self.start <= idx && idx < self.end
    }

    pub(crate) fn len(&self) -> u64 {
        self.end - self.start
    }
}

/// Read handle for a file.
pub(crate) struct AsyncReader {
    volume: String,

    /// offset that represents what we've read so far
    file_cursor: u64,

    /// overall range of the read relative to the volume -- this may span multiple chunks within the
    /// cache
    volume_range: ByteRange,

    /// byte range for reads relative to the `chunk`
    chunk_range: ByteRange,

    /// future that returns the current chunk (bytes) when the read from object storage is done.
    /// if the chunk is already cached, this future resolves immediately.
    chunk: BytesFuture,

    /// if provided, the number of bytes to read ahead for this AsyncReader
    read_ahead_size: Option<u64>,

    /// TODO: handle into the FileChunkCache so we can trigger read-aheads and swap out our chunk
    /// for subsequent chunks
    cache_handle: Option<()>, // placeholder
}

impl AsyncReader {
    pub(crate) async fn read(&mut self) -> Option<bytes::Bytes> {
        if self.finished() {
            return None;
        }

        let rv = self.chunk.clone().await.slice(std::ops::Range {
            start: self.chunk_range.start as usize,
            end: self.chunk_range.end as usize,
        });
        self.file_cursor += self.chunk_range.len();
        self.next().await;

        Some(rv)
    }

    /// Swap out the internal `chunk` for the subsequent chunk. No-op if AsyncReader is finished
    /// reading the entire volume range.
    async fn next(&mut self) {
        if self.finished() {
            return;
        }

        // TODO: use cache_handle to grab the next chunk and replace self.chunk
        unimplemented!()
    }

    /// Return whether the AsyncReader has exhausted the reads within `self.volume_range`.
    fn finished(&self) -> bool {
        self.file_cursor == self.volume_range.end
    }
}

/// Describes a chunk within Volume. Does not map directly to a file, just an arbitrary chunk of
/// bytes within the volume.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct VolumeChunk {
    volume: String,
    range: ByteRange,
}

// TODO: eviction policy, fixed cache size? LRU-2 or something?
pub(crate) struct VolumeChunks {
    /// Cache from the chunk described by VolumeChunk to the future that fetches the data from
    /// object storage.
    read_futures: Arc<Mutex<BTreeMap<VolumeChunk, BytesFuture>>>,
    /// The underlying bytes for the chunk described by VolumeChunk. The bytes are served by the
    /// BytesFuture.
    cache: Arc<RwLock<BTreeMap<VolumeChunk, Bytes>>>,

    /// client to query chunks from volumes
    object_storage_client: Option<()>, // placeholder
}

impl VolumeChunks {
    /// Return an AsyncReader that provides data for the requested volume and byte range. If the
    /// data is cached, the AsyncReader holds a pointer to the cached in-memory data. Otherwise,
    /// the AsyncReader will hold a BytesFuture that will resolve after we fetch the byte-range
    /// from object storage.
    ///
    /// Lookups into the cache relies on BTreeMap::range. For a specific volume and byte
    /// range, we want to check if the VolumeChunk that contains the head of this offset + size
    /// we've requested already exists. We search using a std::ops::RangeTo with a partial end-key
    /// to find cache entries that are less than or equal to the volume + offset. We use that to
    /// find whether the chunk we're looking for exists.
    pub async fn read(
        &mut self,
        volume: String,
        offset: u64,
        size: u64,
        read_ahead_size: Option<u64>,
    ) -> AsyncReader {
        let cache = self.read_futures.lock().expect("poisoned");

        let range = Self::cache_search_range(volume.clone(), offset);
        let mut range = cache.range(range);
        // make sure the entry that was returned actually contains our bytes.
        if let Some((key, value)) = range.next_back()
            && key.range.contains(offset)
        {
            return AsyncReader {
                volume,
                file_cursor: offset,
                volume_range: ByteRange {
                    start: offset,
                    end: offset + size,
                },
                chunk_range: ByteRange {
                    start: offset - key.range.start,
                    end: std::cmp::min(key.range.end, offset - key.range.start + size),
                },
                chunk: value.clone(),
                read_ahead_size,
                cache_handle: None,
            };
        }

        // TODO: populate it.
        unimplemented!()
    }

    /// Returns a range to search the Cache for entries that are less than or equal to the given
    /// volume and offset. A partial key is constructed to be used as the end of the range by using
    /// u64::MAX as the size.
    fn cache_search_range(volume: String, offset: u64) -> std::ops::RangeTo<VolumeChunk> {
        std::ops::RangeTo {
            end: VolumeChunk {
                volume,
                range: ByteRange {
                    start: offset,
                    end: u64::MAX,
                },
            },
        }
    }
}
