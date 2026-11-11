# Artifact storage

Configure exactly one `[storage.<backend>]` table. Filesystem storage is always
available; the standard build also includes S3, GCS and Azure Blob. Other backends
require a build with the corresponding feature from [Cargo.toml](../Cargo.toml).
A configured backend must support reading, writing and object metadata lookup.

## Filesystem

```toml
[storage.fs]
root = "artifacts"
```

Relative paths resolve from the process working directory. Temporary writes use
`.tmp` inside the root unless `atomic_write_dir` is specified. Keep temporary and
final files on the same filesystem for atomic rename. Both directories must be
writable by the process user. Containers need a writable data mount.

## Object storage

```toml
[storage.s3]
endpoint = "https://s3.example.org"
bucket = "python-packages"
region = "us-east-1"
```

Create the bucket separately. Supply credentials using the backend's credential
providers or configuration environment overrides, for example
`PIPR__STORAGE__S3__ACCESS_KEY_ID` and `PIPR__STORAGE__S3__SECRET_ACCESS_KEY`.
GCS uses `storage.gcs.bucket`; Azure Blob uses `storage.azblob.container` and
`account_name`.

Backend options use native OpenDAL configuration names and types. Consult the
service documentation matching the dependency in the project's lockfile when
additional options are needed. Enabling a backend does not qualify its durability,
publication behavior or platform support for every deployment.

## Delivery and persistence

Ready artifacts are checked before upstream pages. A fresh process can therefore
serve a stored artifact even when upstream is unavailable. Cache hits redirect
to a signed URL when the backend supports a suitable header-free GET; otherwise
the proxy streams the stored object. Redirect destinations must be reachable by
clients, not just by the proxy.

Only a missing object is a cache miss. Permission errors, timeouts and other
storage failures return errors rather than triggering an upstream fallback.
Cache misses fetch upstream data while filling storage. Failed or cancelled fills
attempt to abort unfinished writes. A write already completing may publish a
complete object; abrupt termination and backend cleanup failures can leave work
unfinished. Checksum verification is delegated to clients.

Project pages and source mappings use a bounded in-memory cache, lost on restart.
Artifact links carry enough context to recover their source from configured
upstream project pages. If an artifact is missing from storage and its link has
also disappeared upstream, a cold process cannot recover it.

Replicas sharing artifact storage must use matching upstream configuration.
Their memory caches and download coordination remain independent: multiple
replicas may fetch the same artifact concurrently. Backend publication semantics
must support that usage. A local filesystem is shared only when its mount is shared.

## HTTP timeouts

`storage_http` controls connection, response-header and active-read deadlines for
storage HTTP requests, including credential-provider HTTP traffic. See the
configuration template for fields and defaults. There is no whole-file deadline;
time waiting for the consumer does not count as an active storage read.

These limits do not cover filesystem operations, non-HTTP protocols, a complete
multi-request operation or an entire credential-provider chain. A timed-out
request may have reached storage, so timeout alone does not establish whether a
write was published. Cleanup is best effort; shutdown has a separate final bound.

## Custom builds and verification

`cargo build --locked --no-default-features` builds filesystem-only storage.
Add `--features storage-<service>` for other backends. Hyphens in feature names
become underscores in configuration table names. Native/platform-specific
backends can require additional dependencies; `--all-features` is not a portable
verification target.

Filesystem process tests exercise real writes and client downloads. Signing tests
without a live object store do not establish real S3 connectivity or cloud storage
behavior. Validate the chosen service, credentials, redirects and concurrent
writes in your deployment. See [e2e coverage](../e2e/README.md).
