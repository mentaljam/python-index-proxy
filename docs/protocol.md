# Client and HTTP behavior

Paths below are relative to the configured `path_prefix`.
`/{index}/` is an informational page with pip and uv connection examples. It does
not contact upstreams or enumerate projects. Use the trailing slash; the slashless
root is not redirected. Unknown index names return 404.

Project names are case-insensitive and runs of `.`, `_` and `-` normalize to `-`.
Aliases use the same cache and storage context without redirecting. Index names
remain case-sensitive; artifact filenames remain unchanged.

## Package response formats

`/{index}/{package}/` supports `application/vnd.pypi.simple.v1+json`,
`application/vnd.pypi.simple.v1+html`, and legacy `text/html` through `Accept`.
Quality values, wildcards, exclusions (`q=0`), and the `latest` alias are supported.
Without `Accept`, the response remains `text/html`. Equal preferences favor JSON;
an unsupported selection returns 406. Responses vary on `Accept`, independently
of compression negotiation.

Upstream requests prefer JSON and accept HTML as a fallback. Both formats feed a
shared package model; distribution URLs point to the proxy, including `.metadata`
access. JSON responses declare API 1.4 when every contributing source supplies
`versions` and every file has a known `size`. Otherwise they declare API 1.0;
no file requests are made to discover sizes. Versions are combined without
duplicates when all sources provide them. Optional file metadata is preserved
where supported, and a project status is exposed only when all contributing
sources agree. Repository trust declarations (`tracks` and `alternate-locations`)
are not copied from upstreams to the merged proxy repository.

HTML-only upstreams remain supported, including legacy pages with absent or
`text/plain` content types. JSON parsing uses the upstream response content type;
malformed JSON or unsupported JSON major versions fail the refresh with 502,
without caching partial results. An existing empty JSON project returns an empty
file list. The index root remains informational and does not enumerate projects.

## Artifact links and integrity

Artifact links include index, package, source identifier and filename. The
identifier represents the source URL, not a content checksum. Clients cannot
supply an arbitrary upstream URL through this route. Unknown contexts and
mismatched filenames return 404. Metadata requests resolve against the associated
artifact while preserving upstream query parameters.

Checksum fragments and package-selection metadata are passed to clients;
artifact checksum verification is delegated to pip/uv. Cached artifacts can
survive a restart independently of project pages; see [storage](storage.md).

HTML extraction supports common package pages, including unquoted attributes,
character references and raw-text elements. It is not browser-equivalent repair
of arbitrary malformed HTML. Input is expected to be UTF-8; HTML base-element
handling and charset sniffing are not provided.

The informational index root does not implement complete project enumeration.
See [end-to-end tests](../e2e/README.md) for verified scenarios and coverage limits.
