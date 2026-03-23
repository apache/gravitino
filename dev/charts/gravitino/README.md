# Apache Gravitino Helm Chart

This Helm chart deploys Apache Gravitino on Kubernetes with customizable configurations.

## Quick Start

```bash
# Install from OCI registry
helm install gravitino oci://registry-1.docker.io/apache/gravitino-helm --version 1.3.0 -n gravitino --create-namespace

# Or pull the chart first
helm pull oci://registry-1.docker.io/apache/gravitino-helm --version 1.3.0
```

## Documentation

For detailed installation instructions, configuration options, and usage examples, please refer to:

📖 [Complete Helm Chart Documentation](../../../docs/chart.md)

## Quick Links

- [Prerequisites](../../../docs/chart.md#prerequisites)
- [Installation Methods](../../../docs/chart.md#installation-methods)
- [Configuration](../../../docs/chart.md#view-chart-values)
- [Deploy with MySQL](../../../docs/chart.md#deploying-gravitino-with-mysql-as-the-storage-backend)
- [Uninstall](../../../docs/chart.md#uninstall-helm-chart)

## Support

- GitHub Issues: https://github.com/apache/gravitino/issues
- Documentation: https://gravitino.apache.org
- Community: dev@gravitino.apache.org
