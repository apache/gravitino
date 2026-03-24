# Apache Gravitino Iceberg REST Catalog Server Helm Chart

This Helm chart deploys Apache Gravitino Iceberg REST Catalog Server on Kubernetes with customizable configurations.

## Quick Start

```bash
# Install from OCI registry
helm install gravitino-iceberg oci://registry-1.docker.io/apache/gravitino-iceberg-rest-server-helm --version <VERSION> -n gravitino --create-namespace

# Or pull the chart first
helm pull oci://registry-1.docker.io/apache/gravitino-iceberg-rest-server-helm --version <VERSION>
```

## Documentation

For detailed installation instructions, configuration options, and usage examples, please refer to:

📖 [Complete Iceberg REST Catalog Server Documentation](https://github.com/apache/gravitino/blob/main/docs/iceberg-rest-catalog-chart.md)

## Quick Links

- [Prerequisites](https://github.com/apache/gravitino/blob/main/docs/iceberg-rest-catalog-chart.md#prerequisites)
- [Installation](https://github.com/apache/gravitino/blob/main/docs/iceberg-rest-catalog-chart.md#installation)
- [Configuration](https://github.com/apache/gravitino/blob/main/docs/iceberg-rest-catalog-chart.md#view-chart-values)
- [Deploy with Custom Configuration](https://github.com/apache/gravitino/blob/main/docs/iceberg-rest-catalog-chart.md#deploy-with-custom-configuration)
- [Uninstall](https://github.com/apache/gravitino/blob/main/docs/iceberg-rest-catalog-chart.md#uninstall-helm-chart)

## Support

- GitHub Issues: https://github.com/apache/gravitino/issues
- Documentation: https://gravitino.apache.org
- Community: dev@gravitino.apache.org
