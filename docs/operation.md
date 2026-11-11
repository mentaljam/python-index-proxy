# Operation

## Containers

The [root README](../README.md#run-with-docker) shows how to run a published image.
Published releases use `ghcr.io/mentaljam/python-index-proxy:<release-version>`;
select an existing release tag or pin its digest. The release workflow builds
Linux AMD64 and ARM64 images. See [releases](releases.md) for publication.

The container runs as a non-root user (UID/GID 65534:65534) and does not require
privileged mode. Its working directory, `/var/lib/python-index-proxy`, is owned by
that user in the image. Relative storage paths resolve there.

The quick-start example mounts a named volume at that prepared directory. Docker
initializes a new empty volume from the image directory, including its permissions
(unless volume copying is disabled). No host-directory setup or user override is
needed. Preserve the volume across container replacement to retain artifacts.

The application accepts other storage paths. When choosing one, align
`storage.fs.root` and any `atomic_write_dir` with the mount, and ensure both are
writable by UID/GID 65534:65534. Mounted configuration must also be readable.
Bind mounts use host permissions; existing populated volumes keep their own
permissions. The image does not change ownership of mounted data at startup, so
prepare these permissions separately. Changing a mount path does not automatically
change the configured storage path.

The image enables metrics and the standard storage backends. For optional
backends, see [custom container builds](development.md#container-builds).

## Local S3 development

`compose.yaml` starts Silo and a bucket-creation job; it does not run the proxy.
`settings.local.toml` targets that setup and includes example PyTorch sources.
With Docker Compose and the development tools installed:

```sh
docker compose up -d
mise run run
```

Check that the bucket-creation job succeeded before requesting packages.
Credentials in this setup are for local development. A proxy in another container
cannot reach Silo through `127.0.0.1`; configure a reachable service endpoint and
consider client reachability of signed redirects.

## External access

Set `public_url` to the client-facing base URL and align routing with
`path_prefix`; see [configuration](configuration.md). Deploy TLS and access
control at your ingress or reverse proxy. Metrics share the application listener
and route prefix; account for them in exposure rules.

For Kubernetes, use the [chart guide](../charts/python-index-proxy/README.md).
Persistent storage and shared-replica behavior are covered in [storage](storage.md).

## Process shutdown


SIGTERM and SIGINT stop accepting new connections. Existing requests have up to
25 seconds to finish; then requests and artifact fills are cancelled, with up to
5 more seconds for owned tasks and upload cleanup. These periods are fixed.
The process exits early once HTTP connections, downloads and started parsers have
finished. Exit code 0 indicates completed draining; an exceeded final deadline
exits with code 1. A backend operation that remains stuck can prevent cleanup
before this forced exit; unfinished uploads are not guaranteed to be removed.

Local process tests cover idle shutdown, a transfer finishing during grace, and
a stalled transfer cancelled with temporary-file cleanup and a successful retry
after restart. Private subprocess tests cover the hard deadline and blocking
runtime work without adding production test hooks. These checks do not exercise
Kubernetes or cloud storage.


## Troubleshooting

- Startup failure: check the reported configuration field, selected backend and
  build support, and file/data permissions. Diagnostics intentionally omit values.
- Unusable generated links: set `public_url`; a wildcard listen address is not a
  client-facing hostname.
- 404: check the index/package name and source policy; a missing stored artifact
  also needs its source to remain discoverable upstream.
- 502: inspect upstream availability, page-size limits and HTTP timeouts.
- 503 with `Retry-After`: inspect work budgets and concurrent load.
- Failed download after status 200: inspect request `outcome` and `error`, since
  streaming can fail after headers were sent.

Use [logs](logging.md) and [metrics](metrics.md) to distinguish upstream, storage
and response-body failures. A listening TCP port alone does not demonstrate
working storage or upstream connectivity.
