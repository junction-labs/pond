use clap::Parser;

use cfs_fuse::*;

fn main() {
    tracing_subscriber::fmt::init();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let args = Args::parse();

    let res = match args.cmd {
        Cmd::Dump { volume, version } => dump(runtime, volume, version),
        Cmd::Pack { dir, to, version } => pack(runtime, dir, to, version),
        Cmd::List { volume } => list(runtime, volume),
        Cmd::Mount(mount_args) => mount(mount_args),
    };

    if let Err(e) = res {
        eprintln!("{e}");
        std::process::exit(-1);
    }
}
