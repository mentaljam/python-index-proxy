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
