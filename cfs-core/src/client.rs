use std::sync::Arc;

use crate::{
    Error, ErrorKind, Result, Volume, VolumeMetadata,
    cache::{ChunkCache, ReadAheadPolicy},
};

pub struct Client {
    store: crate::storage::Storage,
    cache_size: u64,
    chunk_size: u64,
    readahead: u64,
}

impl Client {
    pub fn new(location: impl AsRef<str>) -> Result<Self> {
        let store = crate::storage::Storage::for_location(location.as_ref())?;

        Ok(Client {
            store,
            // 256 MiB
            cache_size: 256 * 1024 * 1024,
            // 16 MiB
            chunk_size: 16 * 1024 * 1024,
            // 64 MiB
            readahead: 32 * 1024 * 1024,
        })
    }

    pub fn with_cache_size(mut self, size: u64) -> Self {
        self.chunk_size = size;
        self
    }

    pub fn with_chunk_size(mut self, size: u64) -> Self {
        self.chunk_size = size;
        self
    }

    pub fn with_readahead(mut self, size: u64) -> Self {
        self.readahead = size;
        self
    }

    pub async fn list_versions(&self) -> Result<Vec<u64>> {
        self.store.list_versions().await
    }

    pub async fn load_volume(&self, version: Option<u64>) -> Result<Volume> {
        let version = match version {
            Some(version) => version,
            None => self.store.latest_version().await?,
        };
        let metadata = self.store.load_version(version).await?;
        let cache = Arc::new(ChunkCache::new(
            self.cache_size,
            self.chunk_size,
            self.store.clone(),
            ReadAheadPolicy {
                size: self.readahead,
            },
        ));

        Ok(Volume::new(metadata, cache, self.store.clone()))
    }

    pub async fn create_volume(&self, version: u64) -> Result<Volume> {
        let meta_path = self.store.metadata(version);
        if self.store.exists(&meta_path).await? {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                format!("version already exists: {version}"),
            ));
        }

        let metadata = VolumeMetadata::empty();
        let cache = Arc::new(ChunkCache::new(
            self.cache_size,
            self.chunk_size,
            self.store.clone(),
            ReadAheadPolicy {
                size: self.readahead,
            },
        ));

        Ok(Volume::new(metadata, cache, self.store.clone()))
    }
}
