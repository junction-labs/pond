#![deny(clippy::unwrap_used)]

use clap::Parser;

use pond_fs::*;

fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let res = match args.cmd {
        Cmd::List { volume, version } => list(volume, version),
        Cmd::Create {
            dir,
            volume,
            version,
        } => create(dir, volume, version),
        Cmd::Versions { volume } => versions(volume),
        Cmd::Mount(mount_args) => mount(mount_args),
    };

    if let Err(e) = res {
        if args.backtrace {
            // the entire error chain
            write_stderr!("{e:?}");
        } else {
            write_stderr!("{}", fmt_root_cause(e));
        }
        std::process::exit(-1);
    }

    Ok(())
}

fn fmt_root_cause(err: anyhow::Error) -> String {
    if err.source().is_none() {
        err.to_string()
    } else {
        format!("{}\n{}", err, err.root_cause())
    }
}
