#![deny(clippy::unwrap_used)]

use clap::Parser;

use pond_fs::*;

fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let res = match args.cmd {
        Cmd::List { volume, version } => list(runtime, volume, version),
        Cmd::Create {
            dir,
            volume: to,
            version,
        } => create(runtime, dir, to, version),
        Cmd::Versions { volume } => versions(runtime, volume),
        Cmd::Mount(mount_args) => mount(runtime, mount_args),
    };

    if let Err(e) = res {
        if args.backtrace {
            eprintln!("{e:?}");
        } else {
            eprintln!("{e}");
        }
        std::process::exit(-1);
    }

    Ok(())
}
