use bytes::Bytes;
use dashmap::DashMap;
use futures::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use object_store::{ObjectStore, path::Path};
use std::sync::Arc;

/// A boxed future that resolves to bytes.
pub(crate) type BytesFuture = Shared<BoxFuture<'static, bytes::Bytes>>;

/// Similar to std::ops::Range, but the former does not implement ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

impl From<ByteRange> for std::ops::Range<u64> {
    fn from(range: ByteRange) -> Self {
        std::ops::Range {
            start: range.start,
            end: range.end,
        }
    }
}

pub(crate) struct ReadAheadPolicy {
    /// Size of read-ahead in bytes. if you read a byte at index i, we will pre-fetch the byte at
    /// index i + size in the background.
    size: Option<u64>,

    /// Whether we should pre-fetch past the read-range boundary given to the AsyncReader. If
    /// enabled, we will pre-fetch the bytes of the next subsequent file. compaction policy of the
    /// files determines which file will get pre-fetched, but it will always be the data of the
    /// file that is immediately after the file we just read.
    prefetch_past_read_boundary: bool,
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

    /// if provided, performs readaheads based on the given policy
    readahead: Option<ReadAheadPolicy>,

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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    volume: String,
    range: ByteRange,
}

/// Abstraction over Volumes in object storage. Volumes are partitioned up into fixed-size chunks.
/// Requests to read a file (given a volume and byte range) are given an AsyncReader which drives
/// the reads on the volume (wrt read-aheads, caching).
// TODO: eviction policy, fixed cache size? LRU-2 or something?
pub struct VolumeChunks {
    /// Cache from the chunk described by CacheKey to the future that fetches the data from
    /// object storage. The future returns the underlying bytes for the chunk.
    cache: Arc<DashMap<CacheKey, BytesFuture>>,

    /// size of each chunk in bytes
    chunk_size: u64,

    /// object store to query chunks from volumes
    object_store: Arc<dyn ObjectStore>,
}

impl VolumeChunks {
    /// Return an AsyncReader that provides data for the requested volume and byte range. If the
    /// data is cached, the AsyncReader holds a pointer to the cached in-memory data. Otherwise,
    /// the AsyncReader will hold a BytesFuture that will resolve after we fetch the byte-range
    /// from object storage.
    ///
    /// Lookups into the cache relies on BTreeMap::range. For a specific volume and byte
    /// range, we want to check if the CacheKey that contains the head of this offset + size
    /// we've requested already exists. We search using a std::ops::RangeTo with a partial end-key
    /// to find cache entries that are less than or equal to the volume + offset. We use that to
    /// find whether the chunk we're looking for exists.
    pub async fn read(
        &mut self,
        volume: String,
        offset: u64,
        size: u64,
        readahead: Option<ReadAheadPolicy>,
    ) -> AsyncReader {
        let aligned_chunk_start = offset / self.chunk_size * self.chunk_size;
        let key = CacheKey {
            volume: volume.clone(),
            range: ByteRange {
                start: aligned_chunk_start,
                end: aligned_chunk_start + self.chunk_size,
            },
        };

        let chunk_future = match self.cache.entry(key.clone()) {
            // if the chunk isn't cached, shoot off a fetch from object storage and store a future
            // that will resolve when the fetch is done
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let chunk_future = chunk_from_object_store(
                    self.object_store.clone(),
                    entry.key().volume.clone(),
                    entry.key().range,
                )
                .boxed()
                .shared();

                entry.insert(chunk_future.clone());
                chunk_future
            }
            dashmap::mapref::entry::Entry::Occupied(entry) => entry.get().clone(),
        };

        AsyncReader {
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
            chunk: chunk_future,
            readahead,
            cache_handle: None,
        }
    }
}

// TODO: we should drive the future to completion without waiting for the first user to await it
async fn chunk_from_object_store(
    store: Arc<dyn ObjectStore>,
    key: String,
    range: ByteRange,
) -> Bytes {
    let path = Path::from(key);
    store
        .get_range(&path, range.into())
        .await
        .expect("what error handling?")
}
