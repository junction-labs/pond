use std::{io::Write, path::Path, str::FromStr};

use cfs_core::{Ino, Location, volume::Volume};
use clap::{Parser, Subcommand};

#[derive(Parser)]
struct Args {
    #[clap(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Lists available volumes, or the version of a specified volume (e.g. cfs://volume-name)
    List {
        url: Option<String>,
    },

    /// *Experimental* - Pack a directory into an volume blob for testing.
    Pack {
        dir: String,
        volume: String,
        data: String,
    },

    Dump {
        volume: String,
    },
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let client = cfs_client::Client::new();

    match args.cmd {
        Cmd::List { url } => list(&client, url.as_ref()).await,
        Cmd::Pack { dir, volume, data } => pack(&dir, &volume, &data).unwrap(),
        Cmd::Dump { volume } => dump(&volume).unwrap(),
    };
}

async fn list(client: &cfs_client::Client, url: Option<impl AsRef<str>>) {
    match url {
        // list all volumes
        None => {
            let volumes = client.volumes().await.unwrap();
            for volume in volumes {
                println!("{volume}");
            }
        }
        // list versions of a volume
        Some(url) => {
            let url: http::Uri = url.as_ref().parse().unwrap();
            if url.scheme() != Some(&http::uri::Scheme::from_str("cfs").unwrap()) {
                panic!("not a cool url");
            }

            if let Some(volume) = url.authority() {
                let volume = volume.host();

                let versions = client.versions(volume).await.unwrap();
                for version in versions {
                    println!("{version}");
                }
            }
        }
    }
}

fn dump(volume: impl AsRef<Path>) -> anyhow::Result<()> {
    fn location_path(l: &Location) -> String {
        match l {
            Location::Staged(i) => format!("staged({i})"),
            Location::Local { path, .. } => format!("{path}", path = path.display()),
            Location::ObjectStorage { bucket, key, .. } => format!("s3://{bucket}/{key}"),
        }
    }

    let bs = std::fs::read(volume.as_ref()).unwrap();
    let volume = Volume::from_bytes(&bs).unwrap();

    for (name, path, attrs) in volume.walk(Ino::Root).unwrap() {
        let kind;
        let location;
        let offset;
        let len;

        let path = {
            let mut full_path = path;
            full_path.push(name);
            full_path.join("/")
        };
        match attrs.kind {
            cfs_core::FileType::Regular => {
                kind = "f";
                let (l, b) = volume.location(attrs.ino).unwrap();
                location = location_path(l);
                offset = b.offset;
                len = b.len;
            }
            cfs_core::FileType::Directory => {
                kind = "d";
                location = "".to_string();
                offset = 0;
                len = 0;
            }
            cfs_core::FileType::Symlink => todo!(),
        }
        println!("{kind:4} {location:16} {offset:12} {len:8} {path:40}");
    }

    Ok(())
}

fn pack(
    dir: impl AsRef<Path>,
    volume: impl AsRef<Path>,
    data: impl AsRef<Path>,
) -> anyhow::Result<()> {
    use cfs_core::volume::Volume;

    let data = data.as_ref().to_path_buf();
    let root: &Path = dir.as_ref();
    let mut data_f = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&data)
        .unwrap();
    let mut volume_f = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(volume)
        .unwrap();

    let mut volume = Volume::empty();
    let mut cursor = 0u64;

    // create a staging blob
    let staging = Location::Staged(0);

    let walker = walkdir::WalkDir::new(root).min_depth(1);
    for entry in walker {
        let entry = entry?;
        let path = entry.path().strip_prefix(root).unwrap();

        // for a directory, just mkdir_all on the volume
        if entry.file_type().is_dir() {
            let dirs: Vec<_> = path
                .components()
                .map(|c| c.as_os_str().to_string_lossy().to_string())
                .collect();
            volume.mkdir_all(Ino::Root, dirs).unwrap();
        }
        // for a file:
        //
        // - write the content into the blob as bytes
        // - try to open the file (right now with mkdir_all, but it should maybe
        //   be lookup_all if we know this is a dfs?)
        // - write the file into the volume
        //
        // error handling here is interesting: how do we deal with a failure
        // writing the blob? how do we deal with a failure updating the volume?
        // both seem like they're unrecoverable.
        if entry.file_type().is_file() {
            let name = entry.file_name();
            let dir = path.ancestors().nth(1).unwrap();
            let dir_ino = if !dir.to_string_lossy().is_empty() {
                let dirs = dir
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().to_string());
                volume.mkdir_all(Ino::Root, dirs).unwrap().ino
            } else {
                Ino::Root
            };

            let mut file = std::fs::File::open(entry.path()).unwrap();
            let n = std::io::copy(&mut file, &mut data_f).unwrap();
            // eprintln!("adding {path} to blob", path = entry.path().display());
            volume
                .create(
                    dir_ino,
                    name.to_string_lossy().to_string(),
                    true,
                    staging.clone(),
                    cfs_core::ByteRange {
                        offset: cursor,
                        len: n,
                    },
                )
                .unwrap();

            cursor += n;
        }
    }

    volume
        .relocate(
            &staging,
            Location::Local {
                path: data,
                len: cursor as usize,
            },
        )
        .unwrap();

    volume_f.write_all(&volume.to_bytes().unwrap()).unwrap();

    Ok(())
}
