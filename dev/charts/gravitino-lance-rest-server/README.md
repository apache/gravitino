# Apache Gravitino Lance REST Server Helm Chart

This Helm chart deploys Apache Gravitino Lance REST Server on Kubernetes with customizable configurations.

## Quick Start

```bash
# Install from OCI registry
helm install gravitino-lance oci://registry-1.docker.io/apache/gravitino-lance-rest-server-helm --version <VERSION> \
  -n gravitino --create-namespace \
  --set lanceRest.gravitinoUri=http://gravitino:8090 \
  --set lanceRest.gravitinoMetalake=your-metalake

# Or pull the chart first
helm pull oci://registry-1.docker.io/apache/gravitino-lance-rest-server-helm --version <VERSION>
```

## Documentation

For detailed installation instructions, configuration options, and usage examples, please refer to:

📖 [Complete Lance REST Server Documentation](https://github.com/apache/gravitino/blob/main/docs/lance-rest-server-chart.md)

## Quick Links

- [Prerequisites](https://github.com/apache/gravitino/blob/main/docs/lance-rest-server-chart.md#prerequisites)
- [Installation](https://github.com/apache/gravitino/blob/main/docs/lance-rest-server-chart.md#installation)
- [Configuration](https://github.com/apache/gravitino/blob/main/docs/lance-rest-server-chart.md#view-chart-values)
- [Gravitino Backend Configuration](https://github.com/apache/gravitino/blob/main/docs/lance-rest-server-chart.md#configuration-notes)
- [Uninstall](https://github.com/apache/gravitino/blob/main/docs/lance-rest-server-chart.md#uninstall-helm-chart)

## Important Notes

The Lance REST Server requires a running Gravitino instance. Make sure to:
1. Deploy Gravitino first or have an existing Gravitino server
2. Configure `lanceRest.gravitinoUri` to point to your Gravitino server
3. Create a metalake in Gravitino and set `lanceRest.gravitinoMetalake`

## Support

- GitHub Issues: https://github.com/apache/gravitino/issues
- Documentation: https://gravitino.apache.org
- Community: dev@gravitino.apache.org
