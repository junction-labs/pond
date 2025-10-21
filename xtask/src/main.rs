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

    /// Download a prebuilt `flatc` from Github releases.
    ///
    /// Uses the version in `.flatc-version` and the host platform's Cargo
    /// target triple to pick a release URL. Downloads the binary to `./flatc`.
    ///
    /// *NOTE*: The flatc binaries available on Github are all x86-64. If you
    /// can't run an x86-64 binary, you should install the appropriate version
    /// of flatc from source.
    DownloadFlatc,
}

fn main() -> anyhow::Result<()> {
    let project_root = project_root();
    std::env::set_current_dir(&project_root)?;

    let sh = Shell::new()?;
    let args = Args::parse();

    match args.cmd {
        Cmd::CiClippy => ci_clippy(&sh),
        Cmd::Generate => generate_fbs(&sh, "pond"),
        Cmd::CheckUncommitted => check_uncommitted(&sh),
        Cmd::DownloadFlatc => download_flatc(&sh),
    }
}

fn project_root() -> PathBuf {
    Path::new(&env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(1)
        .unwrap()
        .to_path_buf()
}

fn ci_clippy(sh: &Shell) -> anyhow::Result<()> {
    let deny_lints = [
        "warnings",
        "clippy::dbg_macro",
        "clippy::print_stdout",
        "clippy::print_stderr",
        "clippy::todo",
    ];
    let cargo_flags = ["--tests", "--no-deps"];

    let mut clippy_flags = vec![];
    for deny_lint in deny_lints {
        clippy_flags.extend_from_slice(&["-D", deny_lint]);
    }

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

fn generate_fbs(sh: &Shell, crate_name: &str) -> anyhow::Result<()> {
    let fbs = glob::glob(&format!("{crate_name}/fbs/*.fbs"))
        .expect("invalid glob pattern")
        .collect::<Result<Vec<_>, _>>()?;

    cmd!(
        sh,
        "./flatc/flatc -o {crate_name}/src/ --rust --gen-all --filename-suffix '.fbs' {fbs...}"
    )
    .run()
    .map_err(|e| e.into())
}

fn download_flatc(sh: &Shell) -> anyhow::Result<()> {
    // flatc has x86_64 builds for Linux and Mac. try to download them and let
    // it rip, don't bother checking download compatability because the common
    // case here is a dev workstation with some nice fallback to x84_64 through
    // rosetta or something similar.
    let platform = match env!("HOST_PLATFORM") {
        p if p.contains("linux") => "Linux",
        p if p.contains("apple") => "Mac",
        p => anyhow::bail!("No prebuilt flatc for your platform: {p}"),
    };

    let version = read_flatc_version()?;

    let _ = std::fs::remove_dir_all("flatc/");
    std::fs::create_dir("flatc/")?;

    let url = format!(
        "https://github.com/google/flatbuffers/releases/download/{version}/{platform}.flatc.binary.g++-13.zip"
    );
    let curl_opts = [
        // output to flatc/ with the remote filename
        "--output-dir",
        "flatc",
        "--remote-name",
        // follow redirects
        "--location",
    ];
    cmd!(sh, "curl {curl_opts...} {url}").run()?;

    cmd!(sh, "unzip -d flatc/ flatc/Linux.flatc.binary.g++-13.zip").run()?;

    Ok(())
}

fn read_flatc_version() -> anyhow::Result<String> {
    let version = std::fs::read_to_string(".flatc-version")?
        .trim()
        .to_string();
    Ok(version)
}
