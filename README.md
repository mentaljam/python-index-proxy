# Python Index Proxy

A caching proxy for Python package indexes. Give pip and uv one endpoint backed
by ordered upstream sources or a merged index, and reuse downloaded artifacts
across installations. Project pages are cached in memory; artifacts live in a
configured filesystem or object store.

## Install

Install from crates.io with Rust/Cargo:

```sh
cargo install python-index-proxy --locked
```

Alternatively, install from GitHub:

```sh
cargo install --git https://github.com/mentaljam/python-index-proxy --locked
```

To install a specific revision, add `--tag` with an existing release tag.

## Configure

Generate an annotated configuration:

```sh
python-index-proxy --print-settings-template > settings.toml
```

Or save this minimal configuration as `settings.toml`:

```toml
bind = "127.0.0.1:3000"
public_url = "http://127.0.0.1:3000/"

[indices.default]
sources = ["https://pypi.org/simple/{package}/"]

[storage.fs]
root = "artifacts"
```

File settings can be overridden with `PIPR__` environment variables. Exactly one
storage backend is required. See [configuration](docs/configuration.md) and
[storage](docs/storage.md) for details.

## Run locally

```sh
python-index-proxy settings.toml
```

For example, to install `httpx2` through the proxy, run in another terminal:

```sh
pip install --index-url http://127.0.0.1:3000/default/ httpx2
# Or, in an environment managed by uv:
uv pip install --index-url http://127.0.0.1:3000/default/ httpx2
```

Open `http://127.0.0.1:3000/default/` for connection examples. The proxy fetches
packages on demand; it does not mirror or enumerate the entire upstream index.

## Run with Docker

Using the configuration above:

```sh
docker run --rm --name python-index-proxy \
  -p 127.0.0.1:3000:3000 \
  --mount type=bind,src="$(pwd)/settings.toml",dst=/etc/proxy/settings.toml,readonly \
  --mount type=volume,src=python-index-proxy-data,dst=/var/lib/python-index-proxy \
  ghcr.io/mentaljam/python-index-proxy:latest \
  /etc/proxy/settings.toml
```

The container runs as a non-root user and requires no privileged mode. The image
prepares its data directory for that user; Docker initializes a new named volume
at this path with the directory's permissions. Artifact data survives container
removal. The image sets its listen address for container networking; `public_url`
in the configuration keeps client links pointed at localhost.

For other mount paths, bind mounts or existing volumes, ensure the configured
storage directory is writable by the container user. See
[container operation](docs/operation.md) for details.

## Documentation

- [Documentation guide](docs/README.md): configuration, behavior and operation.
- [Kubernetes / Helm chart](charts/python-index-proxy/README.md).
- [Development](docs/development.md) and [end-to-end tests](e2e/README.md).
