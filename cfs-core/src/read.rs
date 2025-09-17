use bytes::Bytes;
use dashmap::DashMap;
use futures::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use object_store::{ObjectStore, path::Path};
use std::sync::Arc;

/// A boxed future that resolves to bytes.
pub(crate) type BytesFuture = Shared<BoxFuture<'static, Bytes>>;

/// Similar to std::ops::Range<u64>.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteRange {
    /// Start of range, inclusive
    pub(crate) start: u64,
    /// End of range, exclusive
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

#[derive(Debug, Clone)]
pub struct ReadAheadPolicy {
    /// Size of read-ahead in bytes. if you read a byte at index i, we will pre-fetch the bytes
    /// within interval [i, i + size) in the background.
    pub(crate) size: u64,
    // Whether we should pre-fetch past the read-range boundary given to the VolumeFile. If
    // enabled, we will pre-fetch the bytes of the next subsequent file. compaction policy of the
    // files determines which file will get pre-fetched, but it will always be the data of the
    // file that is immediately after the file we just read.
    // TODO: prefetch_past_read_boundary: bool,
}

/// Describes a chunk within Volume. Does not map directly to a file, just an arbitrary chunk of
/// bytes within the volume.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct CacheKey {
    volume: String,
    // TODO: maybe this only needs to be a single offset byte? unless we want dynamic chunk sizes
    // later on ...
    range: ByteRange,
}

/// A chunked object store cache.
///
/// Abstraction over Volumes in object storage. Volumes are partitioned up into fixed-size chunks.
/// Read requests (given a volume and byte offset) are served from cache, or fetched from object
/// storage if not already loaded.
pub struct ChunkedVolumeStore {
    /// Cache from the chunk described by CacheKey to the future that fetches the data from
    /// object storage. The future returns the underlying bytes for the chunk.
    // TODO: consider foyer's inmemory cache. it does eviction for us too.
    cache: DashMap<CacheKey, BytesFuture>,

    /// Size of each chunk in bytes
    chunk_size: u64,

    /// Object store to query chunks from volumes
    object_store: Arc<dyn ObjectStore>,
}

impl ChunkedVolumeStore {
    pub fn new(chunk_size: u64, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            cache: DashMap::new(),
            chunk_size,
            object_store,
        }
    }

    /// Return a ByteRange and a BytesFuture that will resolve to a chunk that contains the byte
    /// specified by the given volume and offset byte. The ByteRange represents the absolute
    /// offsets relative to the start of the volume that the BytesFuture reads. If the chunk is
    /// not already cached, fetch it from object storage. If readahead is provided, we will
    /// pre-fetch chunks according to the readahead policy.
    ///
    /// ChunkedVolumeStore::get(...) is thread-safe as the underlying cache is DashMap.
    pub fn get(
        self: &mut Arc<Self>,
        volume: String,
        offset: u64,
        readahead: &Option<ReadAheadPolicy>,
    ) -> (ByteRange, BytesFuture) {
        let key = self.cache_key(volume.clone(), offset);

        let bytes_future = match self.cache.entry(key.clone()) {
            // if the chunk isn't cached, kick off a fetch from object storage and store a future
            // that will resolve when the fetch is done
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let chunk_future = chunk_from_object_store(
                    self.object_store.clone(),
                    entry.key().volume.clone(),
                    entry.key().range,
                )
                .boxed()
                .shared();

                // run this in the background now instead of waiting for the first awaiter
                tokio::spawn(chunk_future.clone());
                entry.insert(chunk_future.clone());

                chunk_future
            }
            dashmap::mapref::entry::Entry::Occupied(entry) => entry.get().clone(),
        };

        if let Some(readahead) = readahead {
            self.readahead(volume.clone(), offset, readahead);
        }

        (key.range, bytes_future)
    }

    /// Perform read-aheads based on the given ReadAheadPolicy. Ensures that the chunks that cover
    /// the byte range starting at offset until offset+readahead are loaded into cache from object
    /// storage.
    ///
    /// ChunkedVolumeStore::readahead(...) is thread-safe as the underlying cache is DashMap.
    fn readahead(self: &mut Arc<Self>, volume: String, offset: u64, readahead: &ReadAheadPolicy) {
        // kick off a read from object store for chunks within the byte range (offset, offset + readahead]
        let last_readahead_byte = offset + readahead.size - 1;
        let start_chunk = (offset / self.chunk_size) * self.chunk_size;
        let end_chunk = (last_readahead_byte / self.chunk_size) * self.chunk_size;
        for chunk_start in (start_chunk..=end_chunk).step_by(self.chunk_size as usize) {
            let key = self.cache_key(volume.clone(), chunk_start);
            if let dashmap::mapref::entry::Entry::Vacant(entry) = self.cache.entry(key) {
                let chunk_future = chunk_from_object_store(
                    self.object_store.clone(),
                    entry.key().volume.clone(),
                    entry.key().range,
                )
                .boxed()
                .shared();

                // run this in the background now instead of waiting for the first awaiter
                tokio::spawn(chunk_future.clone());
                entry.insert(chunk_future.clone());
            }
        }
    }

    /// For a given volume and offset, construct a CacheKey to the chunk that holds the byte
    /// pointed to by the offset.
    fn cache_key(&self, volume: String, offset: u64) -> CacheKey {
        // the start of the chunk that contains offset
        let aligned = offset / self.chunk_size * self.chunk_size;
        CacheKey {
            volume: volume.clone(),
            range: ByteRange {
                start: aligned,
                end: aligned + self.chunk_size,
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn cached(&self, volume: String, offset: u64) -> bool {
        let key = self.cache_key(volume, offset);
        self.cache.contains_key(&key)
    }
}

/// Read a chunk (as specified by the key and byte range) from object storage.
async fn chunk_from_object_store(
    store: Arc<dyn ObjectStore>,
    key: String,
    range: ByteRange,
) -> Bytes {
    let path = Path::from(key);
    // TODO: figure out what happens when the read fails ...
    store
        .get_range(&path, range.into())
        .await
        .expect("what error handling?")
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::Bytes;
    use object_store::{PutPayload, memory::InMemory, path::Path};

    async fn object_store_with_data(key: String, bytes: Bytes) -> Arc<InMemory> {
        let object_store = Arc::new(InMemory::new());
        object_store
            .put(&Path::from(key), PutPayload::from_bytes(bytes))
            .await
            .expect("put into inmemory store should be ok");
        object_store
    }

    #[tokio::test]
    async fn test_readahead() {
        let volume = "volume".to_string();
        let object_store =
            object_store_with_data(volume.clone(), Bytes::from(vec![0u8; 1 << 10])).await;

        // volume store fetches/caches 10 byte chunks
        let mut volume_chunk_store = Arc::new(ChunkedVolumeStore::new(10, object_store));

        // readahead size of 40 bytes (4 chunks)
        let readahead = ReadAheadPolicy { size: 40 };

        let (range, _) = volume_chunk_store.get(volume.clone(), 234, &Some(readahead.clone()));

        // every byte in the readahead-window is cached
        for offset in range.start..(range.start + readahead.size) {
            assert!(volume_chunk_store.cached(volume.clone(), offset));
        }

        // 230 is cached because it's part of the same chunk as 234
        assert!(volume_chunk_store.cached(volume.clone(), 230));
        // 229 is the last byte in the previous chunk, not cached
        assert!(!volume_chunk_store.cached(volume.clone(), 229));
        // 279 is cached because it's part of the same chunk as 234 + 40
        assert!(volume_chunk_store.cached(volume.clone(), 279));
        // 280 is the first byte in the chunk after the read-ahead, not cached
        assert!(!volume_chunk_store.cached(volume.clone(), 280));
    }
}
