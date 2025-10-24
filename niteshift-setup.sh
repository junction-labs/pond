#!/usr/bin/env bash
set -euo pipefail

# niteshift-setup.sh: Setup script for junction-labs/pond
# Generated dependency-driven setup for Rust-based filesystem project

# Logging configuration
log() {
    local message="$1"
    if [[ -n "${NITESHIFT_LOG_FILE:-}" ]]; then
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] $message" >> "$NITESHIFT_LOG_FILE"
    else
        echo "$message"
    fi
}

log_error() {
    local message="$1"
    if [[ -n "${NITESHIFT_LOG_FILE:-}" ]]; then
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $message" >> "$NITESHIFT_LOG_FILE" >&2
    else
        echo "ERROR: $message" >&2
    fi
}

# Check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Verify Rust toolchain is installed
check_rust() {
    log "Checking Rust toolchain..."

    if ! command_exists cargo; then
        log_error "Rust toolchain not found. Please install Rust from https://rustup.rs/"
        log_error "Run: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
        return 1
    fi

    local rust_version
    rust_version=$(rustc --version 2>/dev/null || echo "unknown")
    log "Found Rust: $rust_version"

    local cargo_version
    cargo_version=$(cargo --version 2>/dev/null || echo "unknown")
    log "Found Cargo: $cargo_version"

    return 0
}

# Verify Git is installed (required for build metadata)
check_git() {
    log "Checking Git..."

    if ! command_exists git; then
        log_error "Git not found. Git is required for version information in builds."
        return 1
    fi

    local git_version
    git_version=$(git --version 2>/dev/null || echo "unknown")
    log "Found Git: $git_version"

    return 0
}

# Check for FUSE support (runtime dependency)
check_fuse() {
    log "Checking FUSE support..."

    # Check if FUSE kernel module is available
    if [[ -e /dev/fuse ]]; then
        log "FUSE device /dev/fuse exists"
        return 0
    fi

    # On macOS, check for osxfuse/macfuse
    if [[ "$OSTYPE" == "darwin"* ]]; then
        if command_exists mount_macfuse || command_exists mount_osxfuse; then
            log "macFUSE/OSXFUSE detected"
            return 0
        fi
        log_error "FUSE not found. On macOS, install macFUSE from https://osxfuse.github.io/"
        return 1
    fi

    # On Linux, provide installation guidance
    log_error "FUSE not found. Please install FUSE:"
    log_error "  Ubuntu/Debian: sudo apt-get install fuse3 libfuse3-dev"
    log_error "  Fedora/RHEL:   sudo dnf install fuse3 fuse3-devel"
    log_error "  Arch:          sudo pacman -S fuse3"
    return 1
}

# Check for optional utilities (curl, unzip for flatc download)
check_optional_tools() {
    log "Checking optional tools..."

    local missing_tools=()

    if ! command_exists curl; then
        missing_tools+=("curl")
    fi

    if ! command_exists unzip; then
        missing_tools+=("unzip")
    fi

    if [[ ${#missing_tools[@]} -gt 0 ]]; then
        log "WARNING: Optional tools not found: ${missing_tools[*]}"
        log "These are required for 'cargo xtask download-flatc' but not for basic builds"
    else
        log "Found curl: $(curl --version | head -n1)"
        log "Found unzip: $(unzip -v | head -n1 | awk '{print $2}')"
    fi
}

# Build Rust dependencies
build_dependencies() {
    log "Building Rust dependencies..."

    if [[ ! -f "Cargo.lock" ]]; then
        log_error "Cargo.lock not found. Are you in the pond repository root?"
        return 1
    fi

    log "Running 'cargo fetch' to download dependencies..."
    if ! cargo fetch 2>&1 | while IFS= read -r line; do log "$line"; done; then
        log_error "Failed to fetch Cargo dependencies"
        return 1
    fi

    log "Successfully fetched all Cargo dependencies"
    return 0
}

# Generate FlatBuffers code (idempotent)
generate_flatbuffers() {
    log "Generating FlatBuffers code..."

    # Check if flatc is available or needs to be downloaded
    if ! command_exists flatc; then
        log "flatc not found in PATH, attempting to download..."
        if command_exists curl && command_exists unzip; then
            if ! cargo xtask download-flatc 2>&1 | while IFS= read -r line; do log "$line"; done; then
                log_error "Failed to download flatc"
                return 1
            fi
        else
            log "WARNING: Cannot download flatc (curl/unzip missing). Skipping code generation."
            log "You can install flatc manually from https://github.com/google/flatbuffers/releases"
            return 0
        fi
    fi

    log "Running 'cargo xtask generate' to generate Rust code from schemas..."
    if ! cargo xtask generate 2>&1 | while IFS= read -r line; do log "$line"; done; then
        log_error "Failed to generate FlatBuffers code"
        return 1
    fi

    log "Successfully generated FlatBuffers code"
    return 0
}

# Run initial build to verify setup
verify_build() {
    log "Verifying build configuration..."

    log "Running 'cargo check' to verify all dependencies compile..."
    if ! cargo check --workspace 2>&1 | while IFS= read -r line; do log "$line"; done; then
        log_error "Build verification failed"
        return 1
    fi

    log "Build verification successful"
    return 0
}

# Main setup flow
main() {
    log "=========================================="
    log "Pond Repository Setup"
    log "=========================================="
    log ""

    # Check required tools
    if ! check_rust; then
        exit 1
    fi

    if ! check_git; then
        exit 1
    fi

    # Check FUSE (warn but don't fail)
    if ! check_fuse; then
        log "WARNING: FUSE not available. You can still build pond, but won't be able to mount filesystems."
        log "Continue setup anyway..."
    fi

    # Check optional tools
    check_optional_tools

    log ""
    log "=========================================="
    log "Installing Dependencies"
    log "=========================================="
    log ""

    # Fetch and build dependencies
    if ! build_dependencies; then
        exit 1
    fi

    log ""
    log "=========================================="
    log "Generating Code"
    log "=========================================="
    log ""

    # Generate FlatBuffers code
    if ! generate_flatbuffers; then
        log "WARNING: Code generation had issues, but continuing..."
    fi

    log ""
    log "=========================================="
    log "Verifying Setup"
    log "=========================================="
    log ""

    # Verify build works
    if ! verify_build; then
        exit 1
    fi

    log ""
    log "=========================================="
    log "Setup Complete!"
    log "=========================================="
    log ""
    log "Next steps:"
    log "  - Build:   cargo build --release"
    log "  - Test:    cargo test"
    log "  - Lint:    cargo clippy"
    log "  - Format:  cargo fmt"
    log "  - Install: cargo install --path ./pond-fs"
    log ""
    log "For more information, see README.md"
    log ""
}

# Run main function
main
