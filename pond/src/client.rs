use std::sync::Arc;

use metrics_exporter_prometheus::PrometheusHandle;

use crate::{
    Result, Volume,
    cache::{ChunkCache, ReadAheadPolicy},
    metadata::{Version, VolumeMetadata},
};

pub struct Client {
    store: crate::storage::Storage,
    metrics_handle: Option<PrometheusHandle>,
    cache_size: u64,
    chunk_size: u64,
    readahead: u64,
}

impl Client {
    pub fn new(location: impl AsRef<str>) -> Result<Self> {
        let store = crate::storage::Storage::for_location(location.as_ref())?;

        Ok(Client {
            store,
            metrics_handle: None,
            // 256 MiB
            cache_size: 256 * 1024 * 1024,
            // 16 MiB
            chunk_size: 16 * 1024 * 1024,
            // 64 MiB
            readahead: 32 * 1024 * 1024,
        })
    }

    /// Use metrics_exporter_prometheus::PrometheusHandle to render all recorded metrics in
    /// <root>/.prom/pond.prom. If this is not provided, <root>/.prom/pond.prom will be empty.
    pub fn with_metrics_handle(mut self, handle: PrometheusHandle) -> Self {
        self.metrics_handle = Some(handle);
        self
    }

    pub fn with_cache_size(mut self, size: u64) -> Self {
        self.cache_size = size;
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

    pub async fn list_versions(&self) -> Result<Vec<Version>> {
        self.store.list_versions().await
    }

    pub async fn exists(&self, version: &Version) -> Result<bool> {
        self.store.exists(version).await
    }

    pub async fn load_volume(&self, version: &Option<Version>) -> Result<Volume> {
        let version = match version {
            Some(version) => version,
            None => &self.store.latest_version().await?,
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

        Ok(Volume::new(
            metadata,
            cache,
            self.store.clone(),
            self.metrics_handle.clone(),
        ))
    }

    /// Create a new volume.
    pub async fn create_volume(&self) -> Volume {
        let metadata = VolumeMetadata::empty();
        let cache = Arc::new(ChunkCache::new(
            self.cache_size,
            self.chunk_size,
            self.store.clone(),
            ReadAheadPolicy {
                size: self.readahead,
            },
        ));

        Volume::new(
            metadata,
            cache,
            self.store.clone(),
            self.metrics_handle.clone(),
        )
    }
}
