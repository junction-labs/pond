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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ByteRange {
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
pub(crate) struct ReadAheadPolicy {
    /// Size of read-ahead in bytes. if you read a byte at index i, we will pre-fetch the byte at
    /// index i + size in the background.
    size: u64,
    // Whether we should pre-fetch past the read-range boundary given to the AsyncReader. If
    // enabled, we will pre-fetch the bytes of the next subsequent file. compaction policy of the
    // files determines which file will get pre-fetched, but it will always be the data of the
    // file that is immediately after the file we just read.
    // TODO: prefetch_past_read_boundary: bool,
}

impl Default for ReadAheadPolicy {
    fn default() -> Self {
        Self {
            // 16MiB
            size: 1 << 24,
        }
    }
}

/// Read handle for a file.
// TODO: this only handles reads for a file that lives completely on one volume. if it spans two
// volumes, then we need to keep a list of potential volumes and per-volume ranges that we'll
// iterate over within AsyncReader::next.
pub(crate) struct AsyncReader {
    // TODO: smolstr? lots of cloning happening here.
    volume: String,

    /// Offset that represents what we've read so far in the volume
    read_cursor: u64,

    /// Overall byte range to read in the volume
    overall_read_range: ByteRange,

    /// Byte range for reads in `current_chunk`
    current_chunk_range: ByteRange,

    /// Future that returns the current chunk (bytes) when we've finished reading from object
    /// storage. If the chunk is already cached, this future resolves immediately.
    current_chunk: BytesFuture,

    /// If provided, performs readaheads based on the given policy
    readahead: Option<ReadAheadPolicy>,

    /// Handle into our chunked object store cache.
    volume_chunks: Arc<ChunkedVolumeStore>,
}

impl AsyncReader {
    pub(crate) async fn new(
        mut volume_chunks: Arc<ChunkedVolumeStore>,
        volume: String,
        read_range: ByteRange,
        readahead: Option<ReadAheadPolicy>,
    ) -> Self {
        // read the first chunk into memory
        let (chunk_range, chunk) = volume_chunks.get(volume.clone(), read_range.start, &readahead);
        let relative_chunk_range = ByteRange {
            start: read_range.start - chunk_range.start,
            end: std::cmp::min(read_range.end - chunk_range.start, chunk_range.len()),
        };

        Self {
            volume,
            read_cursor: read_range.start,
            overall_read_range: read_range,
            current_chunk_range: relative_chunk_range,
            current_chunk: chunk,
            readahead,
            volume_chunks,
        }
    }

    pub(crate) async fn read(&mut self) -> Option<Bytes> {
        if self.finished() {
            return None;
        }

        let rv = self.current_chunk.clone().await.slice(std::ops::Range {
            start: self.current_chunk_range.start as usize,
            end: self.current_chunk_range.end as usize,
        });
        self.read_cursor += self.current_chunk_range.len();
        self.load_next_chunk();

        Some(rv)
    }

    /// Swap out the internal `chunk` for the subsequent chunk and optionally perform read-ahead.
    /// no-op if AsyncReader is finished reading the entire volume range.
    fn load_next_chunk(&mut self) {
        if self.finished() {
            return;
        }

        // fetch the next chunk (and do readahead if a policy is given)
        let (chunk_range, chunk) =
            self.volume_chunks
                .get(self.volume.clone(), self.read_cursor, &self.readahead);

        // update the internal current chunk state
        self.current_chunk_range = ByteRange {
            start: self.read_cursor - chunk_range.start,
            end: std::cmp::min(
                self.overall_read_range.end - chunk_range.start,
                chunk_range.len(),
            ),
        };
        self.current_chunk = chunk;
    }

    /// Return whether the AsyncReader has exhausted the reads within self.overall_read_range.
    fn finished(&self) -> bool {
        self.read_cursor == self.overall_read_range.end
    }
}

/// Describes a chunk within Volume. Does not map directly to a file, just an arbitrary chunk of
/// bytes within the volume.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

    /// Return a BytesFuture that will resolve to a chunk that contains the byte specified by the
    /// given volume and offset byte. If the chunk is not already cached, fetch it from object
    /// storage. If readahead is provided, we will pre-fetch chunks according to the readahead
    /// policy.
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
    pub(crate) fn readahead(
        self: &mut Arc<Self>,
        volume: String,
        offset: u64,
        readahead: &ReadAheadPolicy,
    ) {
        // start of the next chunk (the chunk immediately after the one `offset` is in)
        let readahead_offset = offset / self.chunk_size * self.chunk_size + self.chunk_size;
        // kick off a read from object store for chunks within the byte range (offset, offset + readahead]
        for chunk_offset in
            (readahead_offset..readahead_offset + readahead.size).step_by(self.chunk_size as usize)
        {
            let key = self.cache_key(volume.clone(), chunk_offset);
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
    fn cached(&self, volume: String, offset: u64) -> bool {
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
    use object_store::{PutPayload, memory::InMemory};

    fn random_bytes(n: usize) -> Bytes {
        Bytes::from(rand::random_iter().take(n).collect::<Vec<_>>())
    }

    async fn object_store_with_data(key: String, bytes: Bytes) -> Arc<InMemory> {
        let object_store = Arc::new(InMemory::new());
        object_store
            .put(&Path::from(key), PutPayload::from_bytes(bytes))
            .await
            .expect("put into inmemory store should be ok");
        object_store
    }

    #[tokio::test]
    async fn test_asyncreader_full_volume_no_readahead() {
        let volume = "volume".to_string();
        let data = random_bytes(1 << 16); // 64 KiB
        let object_store = object_store_with_data(volume.clone(), data.clone()).await;

        // volume store fetches/caches 1 KiB chunks
        let volume_chunks = Arc::new(ChunkedVolumeStore::new(1 << 10, object_store));

        // read it all up!
        let mut buffer: Vec<u8> = Vec::with_capacity(data.len());
        let mut reader = AsyncReader::new(
            volume_chunks.clone(),
            volume,
            ByteRange {
                start: 0,
                end: data.len() as u64,
            },
            None,
        )
        .await;
        while let Some(data) = reader.read().await {
            buffer.extend(data.iter());
        }

        assert_eq!(data.as_ref(), buffer);
    }

    #[tokio::test]
    async fn test_asyncreader_partial_volume_no_readahead() {
        let volume = "volume".to_string();
        let data = random_bytes(1 << 16); // 64 KiB
        let object_store = object_store_with_data(volume.clone(), data.clone()).await;

        // volume store fetches/caches 1 KiB chunks
        let volume_chunks = Arc::new(ChunkedVolumeStore::new(1 << 10, object_store));

        // read the middle 1/3 of the volume, this is a weird number so partial chunks should be
        // read. this maps to reading bytes (21845..43690] which doesn't get chunked up cleanly!
        let read_range = ByteRange {
            start: (data.len() / 3) as u64,
            end: (2 * data.len() / 3) as u64,
        };
        let mut buffer: Vec<u8> = Vec::with_capacity(read_range.len() as usize);
        let mut reader = AsyncReader::new(volume_chunks.clone(), volume, read_range, None).await;
        while let Some(data) = reader.read().await {
            buffer.extend(data.iter());
        }

        let slice = data.slice(read_range.start as usize..read_range.end as usize);
        assert_eq!(slice.as_ref(), buffer);
    }

    #[tokio::test]
    async fn test_readahead() {
        let volume = "volume".to_string();
        let data = random_bytes(1 << 10); // 1 KiB
        let object_store = object_store_with_data(volume.clone(), data.clone()).await;

        // volume store fetches/caches 10 byte chunks
        let mut volume_chunks = Arc::new(ChunkedVolumeStore::new(10, object_store));

        // readahead size of 40 bytes (4 chunks)
        let readahead = Some(ReadAheadPolicy { size: 40 });

        let (range, _) = volume_chunks.get(volume.clone(), 42, &readahead);
        // every byte from the range is cached
        for offset in range.start..(range.start + 40) {
            assert!(
                volume_chunks.cached(volume.clone(), offset),
                "byte at {offset} was not cached"
            );
        }

        // 40 is cached because it's part of the same chunk as 42
        assert!(volume_chunks.cached(volume.clone(), 40));
        // 39 is the last byte in the previous chunk, not cached
        assert!(!volume_chunks.cached(volume.clone(), 39));
        // 89 is cached because it's part of the same chunk as 42 + 40
        assert!(volume_chunks.cached(volume.clone(), 89));
        // 90 is the first byte in the chunk after the read-ahead, not cached
        assert!(!volume_chunks.cached(volume.clone(), 90));
    }
}
