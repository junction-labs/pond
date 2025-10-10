use object_store::{ObjectStore, aws::AmazonS3Builder, local::LocalFileSystem};
use std::sync::Arc;

use crate::Location;

/// Create an object store for a Location.
///
/// Local filesystem locations will return `Ok(LocalFileSystem)`. Object storage based
/// locations will use environment based credentials where possible and fall
/// back to their default configurations.
pub fn from_location(location: &Location) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
    match location {
        Location::Local { .. } | Location::Staged { .. } => {
            let fs = LocalFileSystem::new();
            Ok(Arc::new(fs))
        }
        Location::ObjectStorage { bucket, .. } => {
            let s3 = AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()?;

            Ok(Arc::new(s3))
        }
    }
}

pub async fn list_versions(
    object_store: &dyn ObjectStore,
    location: &Location,
) -> Result<Vec<u64>, object_store::Error> {
    let res = object_store
        .list_with_delimiter(Some(&location.path()))
        .await?;

    let mut versions = vec![];
    for object in res.objects {
        let Some(filename) = object.location.filename() else {
            continue;
        };

        if let Some(version_str) = filename.strip_suffix(".volume")
            && let Ok(v) = version_str.parse()
        {
            versions.push(v);
        }
    }
    Ok(versions)
}

pub async fn exists(
    object_store: &dyn ObjectStore,
    location: &Location,
) -> Result<bool, object_store::Error> {
    match object_store.head(&location.path()).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(e) => Err(e),
    }
}
