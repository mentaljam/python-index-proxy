# python-index-proxy Helm chart

Deploys the proxy, its configuration and Service, with optional PVC, Ingress,
HTTPRoute and ServiceMonitor. It does not deploy storage services, create buckets,
install CRDs, or configure collectors. CI and image/chart publication are managed
separately.

## Installation

Helm 3 or 4 and Kubernetes 1.25+ are required. Optional integrations require their
own APIs: `gateway.networking.k8s.io/v1` for HTTPRoute and
`monitoring.coreos.com/v1` for ServiceMonitor.

The default image reference is
`ghcr.io/mentaljam/python-index-proxy:<Chart.appVersion>`. The referenced image
must first be built and published; this chart does not publish it. Override
`image.repository`, `image.tag`, or `image.digest` when using another image.
A digest takes precedence over the tag. The repository Dockerfile enables
`fs`, `s3`, `gcs`, `azblob` and metrics by default. Other backends require a custom
build with the corresponding Cargo features. The current Dockerfile targets
x86-64 Linux.

Explicitly configure exactly one backend in `config.storage`. Default values
intentionally fail schema validation until storage is selected:

```sh
helm lint charts/python-index-proxy --strict \
  -f charts/python-index-proxy/examples/s3.yaml
helm template proxy charts/python-index-proxy --namespace packages \
  -f charts/python-index-proxy/examples/s3.yaml
helm install proxy charts/python-index-proxy --namespace packages --create-namespace \
  -f my-values.yaml
```

The examples are templates for your own values: substitute endpoints, bucket names,
public URLs, Secret names and image tags before installing.

After the corresponding release has been published, install directly from GHCR:

```sh
helm install proxy oci://ghcr.io/mentaljam/charts/python-index-proxy \
  --version 0.1.0 --namespace packages --create-namespace -f my-values.yaml
```

The chart's `version` and `appVersion` match the Cargo package version. CI checks
this agreement and the release workflow packages Chart.yaml unchanged. The OCI
artifact contains the entire chart archive, including `values.schema.json`.
Helm uses that embedded schema to validate your values during install, upgrade,
lint and template operations. No separate schema download is needed. The schema
URL comment in values.yaml serves editor validation for copied values files.

## Configuration and credentials

`config` is a YAML structure matching the application's TOML configuration, except
for `bind`, which the chart manages through `containerPort`. Helm converts it to
TOML in a ConfigMap and mounts it at
`/etc/python-index-proxy/python_index_proxy.toml`. The file is passed as the first
application argument. Configuration changes update the Pod template checksum and
trigger a rollout.

```yaml
config:
  public_url: https://packages.example.com/proxy/
  path_prefix: /proxy/
  indices:
    default:
      policy: merge
      sources:
        - https://pypi.org/simple/{package}/
        - https://packages.example.org/simple/{package}/
  storage:
    s3:
      endpoint: https://s3.example.com
      bucket: python-packages
      region: us-east-1
      disable_ec2_metadata: true
```

Use native OpenDAL option names and types for the chosen backend. For example,
GCS uses `storage.gcs.bucket`, and Azure Blob uses `storage.azblob.container` and
`account_name`. The schema recognizes application backend names but leaves their
native options extensible. The application validates backend-specific options and
build support at startup. Do not put credentials or authenticated upstream URLs
in `config`: the generated ConfigMap is not a Secret.

Pass credentials from existing Secrets using `env`:

```yaml
env:
  - name: PIPR__STORAGE__S3__ACCESS_KEY_ID
    valueFrom:
      secretKeyRef:
        name: proxy-storage
        key: access-key-id
  - name: PIPR__STORAGE__S3__SECRET_ACCESS_KEY
    valueFrom:
      secretKeyRef:
        name: proxy-storage
        key: secret-access-key
```

Environment variables override file values. `envFrom` can import an existing
Secret whose keys are already named as the required environment variables:

```yaml
envFrom:
  - secretRef:
      name: proxy-environment
```

The chart creates no Secrets. After rotating an environment-backed Secret,
restart the Pods to load its new values. Do not override `PIPR__BIND`: the chart
sets it explicitly to match the named HTTP port, including when the image has a
default bind environment variable. Keep `path_prefix` in `config` so the generated
ServiceMonitor path stays aligned with application routing. Lists and complex
index configuration belong in `config`, not environment variables.

`serviceAccount` supports creating an annotated account or selecting an existing
one (`create: false`, `name: ...`). Token automount is disabled by default; enable
it explicitly if your authentication setup requires it. Arbitrary extra volume
mounts and file-based credential injection are outside this initial chart.

## Filesystem storage and replicas

```yaml
config:
  storage:
    fs:
      root: /data/artifacts
persistence:
  enabled: true
  size: 20Gi
  accessModes: [ReadWriteOnce]
```

The filesystem data volume is mounted at `persistence.mountPath` (default `/data`).
`fs.root` and any explicit `fs.atomic_write_dir` must be inside that mount. A
persistent fs deployment defaults to `Recreate`, so upgrades have downtime but do
not schedule overlapping old and new Pods against an RWO volume. Set
`updateStrategy` explicitly to choose another strategy.

Set `persistence.existingClaim` to attach an existing PVC without creating one.
Declare its actual access modes in `persistence.accessModes`. Otherwise the chart
creates a PVC with the requested size and optional StorageClass. `storageClass:
null` uses the cluster default; `storageClass: ""` requests no StorageClass.
A chart-created PVC follows Helm's normal deletion lifecycle; use an externally
managed claim when its lifecycle should be independent of the release.

With explicit `storage.fs` and `persistence.enabled: false`, the chart mounts an
`emptyDir`: artifacts are lost when the Pod is replaced. There is no implicit
filesystem backend and no implicit persistent claim.

`replicaCount` is configurable, including zero to scale down. External storage
supports multiple replicas. Filesystem deployments with multiple replicas require
persistence and declared `ReadWriteMany` access; the actual storage driver must
support that access mode. Per-replica in-memory caches and download coordination
remain independent, so replicas can fetch the same artifact concurrently.

`resources` defaults to `{}`; set requests and limits for your workload. Scheduling,
Pod/container security contexts and image pull Secrets are configurable.
The default UID/GID and fsGroup are 65534. Probes use the named HTTP TCP port;
they check that the listener accepts connections, not that upstreams or storage
are healthy. The default 35-second termination grace covers the application's
25-second drain and 5-second cleanup windows. HPA and PDB are not included.

## External access

Both options are disabled by default; enable at most one. For Ingress, combine
storage values with [examples/ingress.yaml](examples/ingress.yaml). Specify class,
hosts, paths, annotations and TLS Secret references. For Gateway API, use
[examples/gateway.yaml](examples/gateway.yaml): HTTPRoute references an existing
Gateway through `parentRefs`; TLS belongs to that Gateway. Cross-namespace
attachment must be allowed by the Gateway.

HTTPRoute matches `httpRoute.path`, defaulting to `config.path_prefix`. It forwards
the path unchanged. Ingress paths likewise must match the application's routing
unless you configure a controller-specific rewrite. Set `config.public_url` to
the client-facing URL; it is not inferred from hosts because routing may include
multiple hosts or external rewrites. No authentication resources are created.
Metrics share the same listener and prefix as application traffic.

## Prometheus and Alloy

Metrics are enabled in the standard container build. Enable `serviceMonitor` to
create one monitor selecting the chart's Service:

```yaml
serviceMonitor:
  enabled: true
  labels:
    release: prometheus
  interval: 30s
  scrapeTimeout: 10s
```

Choose labels matching your collector's selectors; `release: prometheus` is only
an example. Scrape timeout must not exceed interval. The monitor uses the named
Service port and `{config.path_prefix}/metrics`, with one separating slash.
Prometheus Operator can select it, as can an Alloy instance configured with
[`prometheus.operator.servicemonitors`](https://grafana.com/docs/alloy/latest/reference/components/prometheus/prometheus.operator.servicemonitors/).
The ServiceMonitor CRD must already exist, and the collector needs the appropriate
namespace selectors and read permissions. The chart installs neither dependency.

Without ServiceMonitor, configure the collector directly or set
`service.annotations` / `podAnnotations` to match your existing discovery setup.
Annotations only work when that collector is configured to interpret them.
Collect once per target: enabling two discovery mechanisms can duplicate scrapes.

## Validation

`values.schema.json` checks value types, known chart/config fields, backend
selection, index structures and required enabled-route settings. Templates also
check cross-field constraints such as filesystem replicas and mount paths.
Backend-specific semantics and URL/path validation remain the application's
responsibility.

Run `mise run chart-check` for local linting of the supplied storage and routing
examples, or use `helm lint` / `helm template` with your own values. No cluster
access is needed. These checks do not establish image availability, storage
connectivity, or runtime behavior in your Kubernetes cluster.
