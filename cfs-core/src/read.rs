use crate::{ByteRange, Location};
use bytes::Bytes;
use dashmap::DashMap;
use futures::{
    FutureExt,
    future::{BoxFuture, Shared, ready},
};
use object_store::{ObjectStore, aws::AmazonS3Builder, path::Path};
use std::{path::PathBuf, sync::Arc};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

/// A boxed future that resolves to bytes.
pub(crate) type BytesFuture = Shared<BoxFuture<'static, Result<Bytes, ReadError>>>;

#[derive(thiserror::Error, Debug, Clone)]
pub(crate) enum ReadError {
    #[error("generic error: {0}")]
    Generic(String),
    // TODO: take a look at object_store::Error and see how we can map it ourselves. unfortunately
    // object_store::Error doesn't impl Clone so we cannot use #[from] <Error> ... sad
    // but we do need more than just a Genreic error here lol
}

impl From<foyer_memory::Error> for ReadError {
    fn from(err: foyer_memory::Error) -> Self {
        ReadError::Generic(err.to_string())
    }
}

impl From<object_store::Error> for ReadError {
    fn from(err: object_store::Error) -> Self {
        ReadError::Generic(err.to_string())
    }
}

impl From<std::io::Error> for ReadError {
    fn from(err: std::io::Error) -> Self {
        ReadError::Generic(err.to_string())
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReadAheadPolicy {
    /// Size of read-ahead in bytes. if you read a byte at index i, we will pre-fetch the bytes
    /// within interval [i, i + size) in the background.
    pub size: u64,
    // TODO: Whether we should pre-fetch past the read-range boundary given to the VolumeFile. If
    // enabled, we will pre-fetch the bytes of the next subsequent file. compaction policy of the
    // files determines which file will get pre-fetched, but it will always be the data of the
    // file that is immediately after the file we just read.
    // TODO: prefetch_past_read_boundary: bool,
}

/// Describes a chunk within Volume. Does not map directly to a file, just an arbitrary chunk of
/// bytes within the volume.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Chunk {
    location: Location,
    // TODO: maybe this only needs to be a single offset byte? unless we want dynamic chunk sizes
    // later on ...
    range: ByteRange,
}

/// A function for generating a new client based on a bucket name.
pub type ClientBuilder = Arc<dyn Fn(String) -> Arc<dyn ObjectStore> + Send + Sync + 'static>;

/// A chunked object store cache.
///
/// Abstraction over Volumes in object storage. Volumes are partitioned up into fixed-size chunks.
/// Read requests (given a volume and byte offset) are served from cache, or fetched from object
/// storage if not already loaded.
pub struct ChunkCache {
    /// Cache mapping chunks to their materialized bytes.
    cache: foyer::Cache<Chunk, Bytes>,

    /// Size of each chunk in bytes
    chunk_size: u64,

    /// Generate a client
    client_builder: ClientBuilder,

    /// Object store to query chunks from volumes
    clients: Arc<DashMap<String, Arc<dyn ObjectStore>>>,

    /// Global readahead policy. For every get, if readahead is enabled, fetch the bytes within the
    /// readahead window in parallel.
    readahead_policy: ReadAheadPolicy,
}

impl ChunkCache {
    pub fn new(max_cache_size: u64, chunk_size: u64, readahead_policy: ReadAheadPolicy) -> Self {
        let max_cache_size = (max_cache_size / chunk_size) as usize;
        let cache = foyer::Cache::<Chunk, Bytes>::builder(max_cache_size)
            .with_eviction_config(foyer::S3FifoConfig::default())
            .build();

        let default_builder: ClientBuilder = Arc::new(|bucket: String| {
            Arc::new(
                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_region("us-east-2")
                    .build()
                    .unwrap(),
            )
        });

        Self {
            cache,
            chunk_size,
            client_builder: default_builder,
            clients: Arc::new(DashMap::new()),
            readahead_policy,
        }
    }

    #[cfg(test)]
    pub fn new_with(
        chunk_size: u64,
        readahead_policy: ReadAheadPolicy,
        client_builder: ClientBuilder,
    ) -> Self {
        let cache = foyer::Cache::<Chunk, Bytes>::builder(1000000000)
            .with_eviction_config(foyer::S3FifoConfig::default())
            .build();
        Self {
            cache,
            chunk_size,
            client_builder,
            clients: Arc::new(DashMap::new()),
            readahead_policy,
        }
    }

    /// Return a ByteRange and a BytesFuture that will resolve to a chunk that contains the byte
    /// specified by the given volume and offset byte. The ByteRange represents the absolute
    /// offsets relative to the start of the volume that the BytesFuture reads. If the chunk is
    /// not already cached, fetch it from object storage.
    ///
    /// ChunkedVolumeStore::get(...) is thread-safe as the underlying cache is backed by foyer.
    pub(crate) fn get(
        self: &mut Arc<Self>,
        location: Location,
        offset: u64,
    ) -> (ByteRange, BytesFuture) {
        let key = self.chunk(location.clone(), offset);

        let bytes_future = self.read_chunk(key.clone());
        self.readahead(location, offset);

        (key.range, bytes_future)
    }

    /// Perform read-aheads based on the given ReadAheadPolicy. Ensures that the chunks that cover
    /// the byte range starting at offset until offset+readahead are loaded into cache from object
    /// storage.
    ///
    /// ChunkedVolumeStore::readahead(...) is thread-safe as the underlying cache is backed by foyer.
    fn readahead(self: &mut Arc<Self>, location: Location, offset: u64) {
        if self.readahead_policy.size == 0 {
            return;
        }

        // kick off a read from object store for chunks within the byte range (offset, offset + readahead]
        let last_readahead_byte = offset + self.readahead_policy.size - 1;
        let start_chunk = (offset / self.chunk_size) * self.chunk_size;
        let end_chunk = (last_readahead_byte / self.chunk_size) * self.chunk_size;
        for chunk_start in (start_chunk..=end_chunk).step_by(self.chunk_size as usize) {
            let key = self.chunk(location.clone(), chunk_start);
            #[allow(unused_must_use)]
            self.read_chunk(key);
        }
    }

    /// If the chunk is not already in the cache, fetch the bytes from the chunk location
    /// and store it into the cache. Return a shared future that resolves to the bytes within the
    /// chunk.
    ///
    /// The shared future is run immediately, rather than waiting for the first `await`er.
    fn read_chunk(self: &Arc<Self>, chunk: Chunk) -> BytesFuture {
        if let Some(entry) = self.cache.get(&chunk) {
            let bytes = entry.value().clone();
            return ready(Ok(bytes)).boxed().shared();
        }

        let cache = self.cache.clone();
        let location = chunk.location.clone();
        let range = chunk.range;

        let fut = cache
            .fetch(chunk, move || {
                let location = location.clone();
                let clients = self.clients.clone();
                let client_builder = self.client_builder.clone();

                async move {
                    match &location {
                        Location::Staged(_) => unimplemented!(),
                        Location::Local { path, .. } => read_local_chunk(path.clone(), range).await,
                        Location::ObjectStorage { bucket, key, .. } => {
                            let client = get_client(clients, client_builder, bucket.to_string());
                            read_object_store_chunk(client, key.clone(), range).await
                        }
                    }
                }
            })
            .map(|res| res.map(|entry| entry.value().clone()))
            .boxed()
            .shared();

        tokio::spawn(fut.clone());

        fut
    }

    /// For a given volume and offset, construct a Chunk that holds the byte
    /// pointed to by the offset.
    fn chunk(&self, location: Location, offset: u64) -> Chunk {
        // the start of the chunk that contains offset
        let aligned = offset / self.chunk_size * self.chunk_size;
        Chunk {
            location: location.clone(),
            range: ByteRange {
                offset: aligned,
                len: self.chunk_size,
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn cached(&self, location: Location, offset: u64) -> bool {
        let key = self.chunk(location, offset);
        self.cache.contains(&key)
    }
}

fn get_client(
    clients: Arc<DashMap<String, Arc<dyn ObjectStore>>>,
    client_builder: ClientBuilder,
    bucket: String,
) -> Arc<dyn ObjectStore> {
    // FIXME: don't panic on bad build, try not to clone the string every time
    let entry = clients
        .entry(bucket.clone())
        .or_insert_with(|| (client_builder)(bucket));
    entry.value().clone()
}

/// Read a chunk (as specified by the key and byte range) from object storage.
///
/// When used in a Shared<Future<_>>, it is guaranteed to run exactly once.
async fn read_object_store_chunk(
    object_store: Arc<dyn ObjectStore>,
    key: String,
    range: ByteRange,
) -> Result<Bytes, ReadError> {
    let path = Path::from(key);
    Ok(object_store.get_range(&path, range.as_range_u64()).await?)
}

async fn read_local_chunk(path: PathBuf, range: ByteRange) -> Result<Bytes, ReadError> {
    let mut file = File::open(path).await?;
    file.seek(std::io::SeekFrom::Start(range.offset)).await?;

    let mut buf = vec![0u8; range.len as usize];
    let n = file.read(&mut buf).await?;
    buf.truncate(n);

    Ok(Bytes::from(buf))
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
        let object_store =
            object_store_with_data("volume".to_string(), Bytes::from(vec![0u8; 1 << 10])).await;
        let location = Location::ObjectStorage {
            bucket: "".to_string(),
            key: "volume".to_string(),
            len: 1 << 10,
        };

        // readahead size of 40 bytes (4 chunks)
        let readahead = ReadAheadPolicy { size: 40 };

        // volume store fetches/caches 10 byte chunks
        let client_builder: ClientBuilder = Arc::new(move |_| object_store.clone());
        let mut volume_chunk_store =
            Arc::new(ChunkCache::new_with(10, readahead.clone(), client_builder));

        let (range, fut) = volume_chunk_store.get(location.clone(), 234);
        fut.await.unwrap();

        // every byte in the readahead-window is cached
        for offset in range.offset..(range.offset + readahead.size) {
            assert!(volume_chunk_store.cached(location.clone(), offset));
        }

        // 230 is cached because it's part of the same chunk as 234
        assert!(volume_chunk_store.cached(location.clone(), 230));
        // 229 is the last byte in the previous chunk, not cached
        assert!(!volume_chunk_store.cached(location.clone(), 229));
        // 279 is cached because it's part of the same chunk as 234 + 40
        assert!(volume_chunk_store.cached(location.clone(), 279));
        // 280 is the first byte in the chunk after the read-ahead, not cached
        assert!(!volume_chunk_store.cached(location.clone(), 280));
    }

    #[tokio::test]
    async fn test_bad_get_removes_entry() {
        // empty, so ChunkedVolumeStore::get(..) will get an error
        let client_builder: ClientBuilder = Arc::new(|_| Arc::new(InMemory::new()));
        let location = Location::ObjectStorage {
            bucket: "".to_string(),
            key: "fake".to_string(),
            len: 1 << 10,
        };

        let mut volume_chunk_store =
            Arc::new(ChunkCache::new_with(10, Default::default(), client_builder));
        let (_, result) = volume_chunk_store.get(location.clone(), 0);

        // get(..) returns a ReadError and the key+offset is no longer cached.
        let result = result.await;
        assert!(matches!(result, Err(ReadError::Generic(..))));
        assert!(!volume_chunk_store.cached(location, 0));
    }
}
