set positional-arguments
set default-list := true

# Build the proxy; pass --release for an optimized binary.
build *args:
    cargo build --locked "$@"

# Run with the local configuration; pass another configuration path to override it.
run config="settings.local.toml":
    cargo run --locked -- {{quote(config)}}

# Format Rust and Python sources.
fmt:
    cargo fmt --all
    uv run --project e2e --locked --no-python-downloads ruff format e2e

# Check formatting, lint Rust and Python, and check Python types.
check:
    cargo fmt --all -- --check
    cargo clippy --locked --all-targets -- -D warnings
    uv run --project e2e --locked --no-python-downloads ruff format --check e2e
    uv run --project e2e --locked --no-python-downloads ruff check e2e
    uv run --project e2e --locked --no-python-downloads ty check --project e2e

# Run Rust tests; additional arguments are forwarded to Cargo.
test *args:
    cargo test --locked "$@"

# Build the filesystem-only proxy used by the end-to-end tests.
build-e2e:
    cargo build --locked --no-default-features

# Build the proxy and run pip/uv end-to-end tests; accepts pytest arguments.
e2e *args: build-e2e
    uv run --project e2e --locked --no-python-downloads pytest -c e2e/pyproject.toml e2e/tests "$@"
