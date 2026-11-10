# Apache Gravitino Helm Chart

This Helm chart deploys Apache Gravitino on Kubernetes with customizable configurations.

## Quick Start

```bash
# Install from OCI registry
helm install gravitino oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION> -n gravitino --create-namespace

# Or pull the chart first
helm pull oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION>
```

## Documentation

For detailed installation instructions, configuration options, and usage examples, please refer to:

📖 [Complete Helm Chart Documentation](https://github.com/apache/gravitino/blob/main/docs/chart.md)

## Quick Links

- [Prerequisites](https://github.com/apache/gravitino/blob/main/docs/chart.md#prerequisites)
- [Installation](https://github.com/apache/gravitino/blob/main/docs/chart.md#installation)
- [Configuration](https://github.com/apache/gravitino/blob/main/docs/chart.md#view-chart-values)
- [Deploy with MySQL](https://github.com/apache/gravitino/blob/main/docs/chart.md#deploying-gravitino-with-mysql-as-the-storage-backend)
- [Uninstall](https://github.com/apache/gravitino/blob/main/docs/chart.md#uninstall-helm-chart)

## Support

- GitHub Issues: https://github.com/apache/gravitino/issues
- Documentation: https://gravitino.apache.org
- Community: dev@gravitino.apache.org
