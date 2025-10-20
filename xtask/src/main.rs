use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use xshell::{Shell, cmd};

#[derive(Parser)]
struct Args {
    #[clap(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run a strict version of clippy for CI.
    CiClippy,

    /// Generate sources from flatbuffers.
    Generate,

    /// Check to see if the repository is dirty.
    CheckUncommitted,
}

fn main() -> anyhow::Result<()> {
    let project_root = project_root();
    std::env::set_current_dir(&project_root)?;

    let sh = Shell::new()?;
    let args = Args::parse();

    match args.cmd {
        Cmd::CiClippy => ci_clippy(&sh),
        Cmd::Generate => generate_srcs(&sh, "pond"),
        Cmd::CheckUncommitted => check_uncommitted(&sh),
    }
}

fn project_root() -> PathBuf {
    Path::new(&env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(1)
        .unwrap()
        .to_path_buf()
}

fn generate_srcs(sh: &Shell, crate_name: &str) -> anyhow::Result<()> {
    let fbs = glob::glob(&format!("{crate_name}/fbs/*.fbs"))
        .expect("invalid glob pattern")
        .collect::<Result<Vec<_>, _>>()?;

    cmd!(
        sh,
        "flatc -o {crate_name}/src/ --rust --gen-all --filename-suffix '.fbs' {fbs...}"
    )
    .run()
    .map_err(|e| e.into())
}

fn ci_clippy(sh: &Shell) -> anyhow::Result<()> {
    let cargo_flags = ["--tests", "--no-deps"];
    let clippy_flags = [
        // deny all warnings
        "-D",
        "warnings",
        // don't allow dbg
        "-D",
        "clippy::dbg_macro",
    ];

    cmd!(sh, "cargo clippy {cargo_flags...} -- {clippy_flags...}").run()?;
    Ok(())
}

fn check_uncommitted(sh: &Shell) -> anyhow::Result<()> {
    let changed = cmd!(sh, "git status --porcelain").read()?;
    if !changed.is_empty() {
        anyhow::bail!("found uncomitted changes: \n\n{changed}");
    }

    Ok(())
}
