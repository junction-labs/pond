use crate::{
    Result, Volume,
    cache::{CacheConfig, ChunkCache},
    metadata::{Version, VolumeMetadata},
};

pub struct Client {
    store: crate::storage::Storage,
    metrics_snapshot_fn: Option<Box<dyn Fn() -> Vec<u8> + Send>>,
    cache_config: CacheConfig,
}

impl Client {
    /// Returns a Client for the Volume at the given location.
    pub fn open(location: impl AsRef<str>) -> Result<Self> {
        let store = crate::storage::Storage::for_location(location.as_ref(), false)?;

        Ok(Client {
            store,
            metrics_snapshot_fn: None,
            cache_config: CacheConfig::default(),
        })
    }

    /// Returns a Client for the Volume at the given location.
    ///
    /// For locations that reference a local volume (using LocalFilesystem), we will
    /// create the volume parent dirs if they do not exist.
    pub fn create(location: impl AsRef<str>) -> Result<Self> {
        let store = crate::storage::Storage::for_location(location.as_ref(), true)?;

        Ok(Client {
            store,
            metrics_snapshot_fn: None,
            cache_config: CacheConfig::default(),
        })
    }

    /// Renders Prometheus metrics to `<root>/.prom/pond.prom` using the given function.
    /// If unset, the file will be empty.
    pub fn with_metrics_snapshot_fn(mut self, f: Box<dyn Fn() -> Vec<u8> + Send>) -> Self {
        self.metrics_snapshot_fn = Some(f);
        self
    }

    pub fn with_cache_config(mut self, config: CacheConfig) -> Self {
        self.cache_config = config;
        self
    }

    pub fn with_cache_size(mut self, size: u64) -> Self {
        self.cache_config.max_cache_size_bytes = size;
        self
    }

    pub fn with_chunk_size(mut self, size: u64) -> Self {
        self.cache_config.chunk_size_bytes = size;
        self
    }

    pub fn with_readahead(mut self, size: u64) -> Self {
        self.cache_config.readahead_size_bytes = size;
        self
    }

    pub async fn list_versions(&self) -> Result<Vec<Version>> {
        self.store.list_versions().await
    }

    pub async fn exists(&self, version: &Version) -> Result<bool> {
        self.store.exists(version).await
    }

    pub async fn load_volume(&mut self, version: &Option<Version>) -> Result<Volume> {
        let version = match version {
            Some(version) => version,
            None => &self.store.latest_version().await?,
        };
        let metadata = self.store.load_version(version).await?;
        let cache = ChunkCache::new(self.cache_config.clone(), self.store.clone());

        Ok(Volume::new(
            metadata,
            cache,
            self.store.clone(),
            self.metrics_snapshot_fn.take(),
        ))
    }

    /// Create a new volume.
    pub async fn create_volume(&mut self) -> Volume {
        let metadata = VolumeMetadata::empty();
        let cache = ChunkCache::new(self.cache_config.clone(), self.store.clone());

        Volume::new(
            metadata,
            cache,
            self.store.clone(),
            self.metrics_snapshot_fn.take(),
        )
    }
}
