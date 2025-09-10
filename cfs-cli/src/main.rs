use std::str::FromStr;

use cfs_md::VolumeInfo;
use clap::{Parser, Subcommand};

#[derive(Parser)]
struct Args {
    #[clap(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    List { url: Option<String> },
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let client = reqwest::Client::new();

    match args.cmd {
        Cmd::List { url } => list(&client, url.as_ref()).await,
    };
}

async fn list(client: &reqwest::Client, url: Option<impl AsRef<str>>) {
    match url {
        // list all volumes
        None => {
            let resp = client
                .get("http://localhost:8888/volumes")
                .send()
                .await
                .unwrap();

            let resp = resp.error_for_status().unwrap();
            let bs = resp.bytes().await.unwrap();

            let info = VolumeInfo::from_bytes(&bs).unwrap();
            for name in info.names() {
                println!("{name}");
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

                let resp = client
                    .get(&format!("http://localhost:8888/volumes/{volume}"))
                    .send()
                    .await
                    .unwrap();

                let resp = resp.error_for_status().unwrap();
                let bs = resp.bytes().await.unwrap();

                let info = VolumeInfo::from_bytes(&bs).unwrap();
                for version in info.versions() {
                    println!("{version}");
                }
            }
        }
    }
}
