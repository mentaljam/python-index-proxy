# Configuration

Pass a TOML configuration file as the first command-line argument. Without an
argument, the application looks for an optional `python_index_proxy` configuration
file in the working directory.

Environment variables with the `PIPR__` prefix override values from the file.
Use double underscores (`__`) after `PIPR` and between nested fields. Single
underscores within field names are preserved:

```text
PIPR__BIND=0.0.0.0:3000
PIPR__LOG_FORMAT=json
PIPR__LIMITS__MAX_INDEX_REFRESHES=8
PIPR__STORAGE_HTTP__READ_TIMEOUT=30s
PIPR__STORAGE__S3__ACCESS_KEY_ID=example-key
PIPR__STORAGE__S3__SECRET_ACCESS_KEY=example-secret
```

For example, keep the S3 endpoint, bucket and region in TOML and supply the last
two variables from a Kubernetes Secret. Environment values are not interpolated
into TOML; they override the corresponding fields after the file is read.
Numeric and boolean fields accept their string representations, while string
fields retain their literal values, including leading zeroes.

Exactly one storage backend must be configured, and it must be enabled in the
application build. Use TOML for structured values such as index source lists;
environment variables do not parse JSON or delimited lists.


## Starting configuration

Use the [minimal working example](../README.md#run-locally), or generate an annotated
configuration from your build:

```sh
python-index-proxy --print-settings-template > settings.toml
```

Template generation requires the `config-template` build feature, enabled by
default. The generated template documents fields and defaults; it is not a
runtime fallback. Adjust sources and storage before deployment.

## Index sources

```toml
[indices.default]
policy = "first"
sources = [
    "https://packages.example.org/simple/{package}/",
    "https://pypi.org/simple/{package}/",
]
```

Each HTTP(S) source template must contain exactly one `{package}` in its path.
Index names are case-sensitive ASCII names with letters, digits and internal
periods, underscores or hyphens. Package names are normalized independently.

- `first` (default): use the first successful project page, even if it has no files.
- `merge`: combine all successful source pages into one project response.

A source array is shorthand for `first`. HTTP 4xx responses except 408 and 429
skip that source; other upstream failures return 502 without caching a partial
result. If no source supplies the project, the response is 404. Source order
therefore matters with `first`; it is not a fallback for individual missing files
or incompatible versions in an otherwise successful project page.

## Listen address and public URL

`bind` sets the listener; `path_prefix` sets the application route prefix.
`public_url` sets the external HTTP(S) base used in generated links and client
commands. It must not contain credentials, query parameters or a fragment.
Trailing slashes are added to both URL settings if absent.

For a reverse proxy that strips `/proxy/` before forwarding:

```toml
bind = "0.0.0.0:3000"
path_prefix = "/"
public_url = "https://packages.example.org/proxy/"
```

Without `public_url`, links use `http://{bind}{path_prefix}`, including wildcard
bind addresses. Request headers do not determine the public URL. Configure it
explicitly whenever clients use another host, HTTPS or a rewritten path.

## Cache, budgets and timeouts

The generated template contains current defaults for these sections:

| Section | Purpose |
| --- | --- |
| `index_cache` | Project-page TTL and byte capacity; reads do not extend TTL |
| `limits` | Decoded upstream page size, concurrent refreshes and artifact fills |
| `upstream` | Connection, headers, active reads and total project-page download timeouts |
| `storage_http` | Independent HTTP timeouts for storage and credential providers |

Cache and work budgets apply per replica. Cache capacity is not a process memory
limit: in-flight work and allocation overhead also consume memory. An oversized
cache entry can serve its current request without being retained. The decoded
page-size limit rejects oversized upstream input with 502.

Requests for the same in-flight object share work. New independent work beyond a
budget receives 503 with `Retry-After: 1`; cache hits remain available. There is
no whole-artifact deadline while data continues to arrive. Storage-specific
limitations are described in [storage](storage.md).

See [logging](logging.md) for `log_format` and `RUST_LOG`.
