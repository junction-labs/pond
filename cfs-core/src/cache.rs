use bytes::Bytes;
use object_store::ObjectStore;
use std::{ops::Range, sync::Arc};

#[derive(thiserror::Error, Debug)]
pub(crate) enum CacheError {
    #[error(transparent)]
    Cache(#[from] foyer_memory::Error),

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
}

#[derive(Debug, Clone, Default)]
pub struct ReadAheadPolicy {
    /// Size of read-ahead in bytes. if you read a byte at index i, we will
    /// pre-fetch the bytes within interval [i, i + size) in the background.
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Chunk {
    path: object_store::path::Path,
    offset: u64,
}

pub struct ChunkCache {
    inner: Arc<ChunkCacheInner>,
}

struct ChunkCacheInner {
    client: Arc<dyn ObjectStore>,
    cache: foyer::Cache<Chunk, Bytes>,
    chunk_size: u64,
    readahead_policy: ReadAheadPolicy,
}

impl ChunkCache {
    pub fn new(
        max_cache_size: u64,
        chunk_size: u64,
        object_store: Arc<dyn ObjectStore>,
        readahead_policy: ReadAheadPolicy,
    ) -> Self {
        let max_cache_size = (max_cache_size / chunk_size) as usize;
        let cache = foyer::Cache::<Chunk, Bytes>::builder(max_cache_size)
            .with_eviction_config(foyer::S3FifoConfig::default())
            .build();

        let inner = ChunkCacheInner {
            cache,
            chunk_size,
            readahead_policy,
            client: Arc::from(object_store),
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn clear(&self) {
        self.inner.cache.clear()
    }

    pub async fn get_at(
        &self,
        path: &object_store::path::Path,
        offset: u64,
        len: u64,
    ) -> Result<Vec<Bytes>, CacheError> {
        let mut chunks = vec![];
        let read_chunks: Vec<_> = self.inner.read_chunks(offset, len).collect();
        for offset in &read_chunks {
            let cache = self.inner.clone();
            let path = path.clone();
            let offset = *offset;
            chunks.push(tokio::spawn(async move {
                cache.fetch_aligned(path, offset).await
            }));
        }

        // touch any readahead chunks. this gets done in background tasks
        // without waiting for them here.
        for offset in self.inner.readahead_chunks(offset + len) {
            let cache = self.inner.clone();
            let path = path.clone();
            tokio::spawn(async move { cache.fetch_aligned(path, offset).await });
        }

        // actually fetch the chunks in parallel here
        //
        // TODO: should we spawn these tasks as well to make sure they're all
        // being fetched on separate threads if possible?
        // TODO: we should figure out what order errors come back in here or if
        // we want all of them. any error can (and should!) kill the whole fetch
        // though.
        let mut chunks: Vec<_> = match futures::future::try_join_all(chunks).await {
            Ok(chunks_res) => chunks_res.into_iter().collect::<Result<_, _>>()?,
            Err(join_err) => panic!("tasks failed: {join_err}"),
        };

        // trim the first chunk so that it starts at the start of the requested
        // range, instead of at the start of an aligned chunk. using Bytes::slice
        // here means we're just taking a ref-counted view into the cache and not
        // actually copying the range.
        if let Some(first_chunk) = chunks.first_mut() {
            let chunk_offset = read_chunks[0];
            let slice_start = (offset - chunk_offset) as usize;
            *first_chunk = first_chunk.slice(slice_start..);
        }
        // trim the last chunk so that it ends at the end of the requested range,
        // or the end of the object - whichever is shorter - instead of ending
        // at a chunk alignment boundary.
        //
        // slicing Bytes means that we're taking a ref-counted view into the cache
        // instead of making a copy of the range.
        if let Some(last_chunk) = chunks.last_mut() {
            let chunk_offset = read_chunks[read_chunks.len() - 1];
            let object_end = offset + len;
            let slice_end = std::cmp::min((object_end - chunk_offset) as usize, last_chunk.len());
            *last_chunk = last_chunk.slice(..slice_end);
        }

        Ok(chunks)
    }
}

impl ChunkCacheInner {
    /// fetch `self.chunk_size` bytes starting at the given offset. assumes
    /// offsets have already been aligned to `self.chunk_size` and panics
    /// if they have not.
    async fn fetch_aligned(
        &self,
        path: object_store::path::Path,
        offset: u64,
    ) -> Result<Bytes, CacheError> {
        assert!(offset.is_multiple_of(self.chunk_size));

        let range = offset..(offset + self.chunk_size);
        let chunk = Chunk {
            path: path.clone(),
            offset,
        };

        let entry = self
            .cache
            .fetch(chunk, || get_range(self.client.clone(), path, range))
            .await?;
        Ok(entry.value().clone())
    }

    #[cfg(test)]
    fn is_cached(&self, path: object_store::path::Path, offset: u64) -> bool {
        let offset = (offset / self.chunk_size) * self.chunk_size;
        let chunk = Chunk { path, offset };
        self.cache.contains(&chunk)
    }

    fn read_chunks(&self, offset: u64, len: u64) -> impl Iterator<Item = u64> {
        let last_byte = offset + len - 1;
        let start_chunk = (offset / self.chunk_size) * self.chunk_size;
        let end_chunk = (last_byte / self.chunk_size) * self.chunk_size;
        (start_chunk..=end_chunk).step_by(self.chunk_size as usize)
    }

    fn readahead_chunks(&self, offset: u64) -> impl Iterator<Item = u64> {
        if self.readahead_policy.size == 0 {
            return (0..=0).step_by(1);
        }

        let last_readahead_byte = offset + self.readahead_policy.size - 1;
        let start_chunk = (offset / self.chunk_size) * self.chunk_size;
        let end_chunk = (last_readahead_byte / self.chunk_size) * self.chunk_size;
        (start_chunk..=end_chunk).step_by(self.chunk_size as usize)
    }
}

// NOTE: keeping this outlined makes rustc happy about move and local references
// where inlining it makes helllllllllla problems.
async fn get_range(
    client: Arc<dyn ObjectStore>,
    path: object_store::path::Path,
    range: Range<u64>,
) -> Result<Bytes, CacheError> {
    let bs = client.get_range(&path, range).await?;
    Ok(bs)
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::Bytes;
    use object_store::{PutPayload, memory::InMemory, path::Path};

    async fn object_store_with_data(
        key: object_store::path::Path,
        bytes: Bytes,
    ) -> Arc<dyn ObjectStore> {
        let object_store = Arc::new(InMemory::new());
        object_store
            .put(&Path::from(key), PutPayload::from_bytes(bytes))
            .await
            .expect("put into inmemory store should be ok");
        object_store
    }

    #[tokio::test]
    async fn test_readahead() {
        let key = object_store::path::Path::from("some-key");
        let object_store =
            object_store_with_data(key.clone(), Bytes::from(vec![0u8; 1 << 10])).await;

        // volume store fetches/caches 10 byte chunks with a readahead size of
        // 40 bytes (4 chunks)
        let chunk_size = 10;
        let cache = ChunkCache::new(1024, 10, object_store, ReadAheadPolicy { size: 40 });
        let read_offset = 123;
        let read_len = 234;
        let last_cached_byte =
            // align to the start of the last chunk
            (read_offset + read_len + cache.inner.readahead_policy.size) / chunk_size * chunk_size
            // and find the last byte
                + (chunk_size - 1);

        let _ = cache.get_at(&key, 123, 234).await.unwrap();

        // every byte in the readahead-window is cached
        for offset in read_offset..=last_cached_byte {
            assert!(
                cache.inner.is_cached(key.clone(), offset),
                "offset should be cached: {offset}"
            );
        }
        // bytes beyond are not cached
        for offset in (last_cached_byte + 1)..(last_cached_byte + chunk_size) {
            assert!(
                !cache.inner.is_cached(key.clone(), offset,),
                "offset should not be cached: {offset}"
            );
        }
    }

    #[tokio::test]
    async fn test_bad_get_removes_entry() {
        let object_store = Arc::new(InMemory::new());
        let cache = ChunkCache::new(10, 10, object_store, ReadAheadPolicy { size: 123 });
        let res = cache.get_at(&"some-key".into(), 10, 37).await;
        assert!(matches!(res, Err(CacheError::ObjectStore(_))));
        assert!(!cache.inner.is_cached("some-key".into(), 10));
    }
}
