# Documentation

Start with the [project README](../README.md) for a working local or Docker setup.

| Task                                                      | Guide                                                |
| --------------------------------------------------------- | ---------------------------------------------------- |
| Configure sources, URLs, limits and environment overrides | [Configuration](configuration.md)                    |
| Choose storage and understand cache persistence           | [Storage](storage.md)                                |
| Run containers, expose the service and diagnose failures  | [Operation](operation.md)                            |
| Understand client compatibility and HTTP behavior         | [Protocol](protocol.md)                              |
| Collect and interpret logs                                | [Logging](logging.md)                                |
| Scrape and interpret metrics                              | [Metrics](metrics.md)                                |
| Deploy to Kubernetes                                      | [Helm chart](../charts/python-index-proxy/README.md) |
| Change the code and run checks                            | [Development](development.md)                        |
| Run client and process tests                              | [End-to-end tests](../e2e/README.md)                 |
| Prepare and publish a release                             | [Releases](releases.md)                              |

These Markdown files are the maintained documentation for users and contributors,
including coding agents. Tool and dependency versions belong in manifests and
lockfiles; generate the configuration template from the binary being deployed.
