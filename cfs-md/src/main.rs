mod volume;
mod volume_info;

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    routing::get,
};
use bytes::Bytes;

use crate::{volume::Volume, volume_info::VolumeInfo};

#[tokio::main]
async fn main() {
    let volumes = Arc::new(Mutex::new(VolumeIndex::from_static([
        (
            "now_thats_what_i_call_storage",
            "vol1",
            "s3://good-bucket/vol1",
        ),
        (
            "now_thats_what_i_call_storage",
            "vol2",
            "s3://good-bucket/vol2",
        ),
        (
            "now_thats_what_i_call_storage",
            "vol3",
            "s3://gooder-bucket/vol3",
        ),
        ("eurovision_winners", "2025", "s3://eurovision/"),
    ])));

    let app = Router::new()
        .route("/volumes", get(list_volumes))
        .route("/volumes/{name}", get(list_versions))
        .route("/volumes/{name}/{version}", get(get_volume))
        .with_state(InMemoryServer { volumes });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8888").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn list_volumes(State(server): State<InMemoryServer>) -> Result<Bytes, StatusCode> {
    let volumes = server.volumes.lock().unwrap();
    Ok(volumes.list().as_bytes())
}

async fn list_versions(
    State(server): State<InMemoryServer>,
    Path(volume): Path<String>,
) -> Result<Bytes, StatusCode> {
    let volumes = server.volumes.lock().unwrap();

    match volumes.list_versions(&volume) {
        Some(versions) => Ok(versions.as_bytes()),
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn get_volume(
    State(server): State<InMemoryServer>,
    Path((name, version)): Path<(String, String)>,
) -> Result<Bytes, StatusCode> {
    let volumes = server.volumes.lock().unwrap();
    volumes
        .get(&name, &version)
        .map(|volume| volume.as_bytes())
        .ok_or(StatusCode::NOT_FOUND)
}

#[derive(Clone)]
struct InMemoryServer {
    volumes: Arc<Mutex<VolumeIndex>>,
}

struct VolumeIndex {
    // name -> version (sorted) -> volume
    volumes: HashMap<String, BTreeMap<String, Volume>>,
}

impl VolumeIndex {
    fn from_static(
        static_volumes: impl IntoIterator<Item = (&'static str, &'static str, &'static str)>,
    ) -> Self {
        let mut volumes = HashMap::new();

        for (name, version, s3_location) in static_volumes {
            let volume = Volume::new(name, version, s3_location);
            let versions: &mut BTreeMap<_, _> = volumes.entry(name.to_string()).or_default();
            versions.insert(version.to_string(), volume);
        }

        Self { volumes }
    }

    fn get(&self, name: &str, version: &str) -> Option<Volume> {
        let versions = self.volumes.get(name)?;
        versions.get(version).cloned()
    }

    fn list(&self) -> VolumeInfo {
        VolumeInfo::names_from_iter(self.volumes.keys())
    }

    fn list_versions(&self, name: &str) -> Option<VolumeInfo> {
        let versions = self.volumes.get(name)?;
        Some(VolumeInfo::versions_from_iter(versions.keys()))
    }
}
