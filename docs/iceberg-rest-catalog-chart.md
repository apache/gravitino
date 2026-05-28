---
title: "Install Iceberg REST Catalog Server on Kubernetes"
slug: "/iceberg-rest-catalog-chart"
keyword:
  - Iceberg REST Helm Chart
license: "This software is licensed under the Apache License version 2."
---

Deploy the Apache Gravitino Iceberg REST catalog server on Kubernetes with its dedicated Helm chart. Values in `values.yaml` and overrides in `gravitino-iceberg-rest-server.conf` are both customizable through Helm.

The chart deploys the Iceberg REST server (IRC) as a separate process. Two of the three IRC deployment modes are reachable through this chart: Bare IRC as the default install, and Ungoverned IRC through values overrides. The third mode, Governed IRC, runs the IRC inside the Gravitino server's JVM as an auxiliary service and is not deployable through this chart; for Governed IRC, install the main Gravitino chart with `auxService.names=iceberg-rest` enabled. See [Iceberg REST Catalog Service > Deployment Modes](iceberg-rest-service.md#deployment-modes) for the side-by-side comparison.

The default install (no values overrides) deploys Bare IRC with an in-memory catalog backend. The in-memory backend loses all tables on pod restart and provides no Gravitino governance: no authentication, no authorization, no Gravitino-orchestrated table maintenance. Use the default install for evaluation only. For any deployment intended to retain state, configure a persistent catalog backend or switch to Ungoverned IRC (or use the main Gravitino chart for Governed IRC).

## Prerequisites

- Kubernetes 1.29+
- Helm 3+
- For Ungoverned IRC only: a running Apache Gravitino server reachable from the Kubernetes cluster, with a metalake created before this chart is deployed. Bare IRC has no Gravitino server prerequisite.

## Sources

### Install from OCI Registry (Recommended for Released Versions)

Pull the chart from Docker Hub OCI registry:

```console
helm pull oci://registry-1.docker.io/apache/gravitino-iceberg-rest-server-helm --version <VERSION>
```

Or install directly:

```console
helm upgrade --install gravitino-iceberg oci://registry-1.docker.io/apache/gravitino-iceberg-rest-server-helm --version <VERSION> -n gravitino --create-namespace
```

### Install from Local Repository (for Development or Unreleased Versions)

Clone the repository and navigate to the chart directory:

```console
git clone https://github.com/apache/gravitino.git
cd gravitino/dev/charts
```

Update chart dependencies:

```console
helm dependency update gravitino-iceberg-rest-server
```

Install the chart:

```console
helm upgrade --install gravitino-iceberg ./gravitino-iceberg-rest-server -n gravitino --create-namespace
```

## Chart Values

Override chart defaults by customizing parameters in `values.yaml`. Configuration in [gravitino-iceberg-rest-server.conf](../dev/charts/gravitino-iceberg-rest-server/resources/gravitino-iceberg-rest-server.conf) can also be modified through Helm `values.yaml`.

To display the default values for the chart, run:

```console
helm show values oci://registry-1.docker.io/apache/gravitino-iceberg-rest-server-helm --version <VERSION>
```

## Deployment

```console
helm upgrade --install [RELEASE_NAME] oci://registry-1.docker.io/apache/gravitino-iceberg-rest-server-helm --version <VERSION> [flags]
```

### Deploy in Bare Mode (Default)

Bare IRC is the default mode: the IRC runs as a standalone process with no Gravitino server in the loop. The default install uses an in-memory catalog backend that loses all tables on pod restart. Bare IRC provides no authentication, no authorization, and no Gravitino-orchestrated table maintenance.

To deploy with the default settings:

```console
helm upgrade --install gravitino-iceberg oci://registry-1.docker.io/apache/gravitino-iceberg-rest-server-helm --version <VERSION> \
  -n gravitino \
  --create-namespace \
  --set replicas=2 \
  --set resources.requests.memory="4Gi" \
  --set resources.requests.cpu="2"
```

For a persistent Bare IRC deployment, configure a non-memory catalog backend (JDBC or Hive) via `icebergRest.catalogBackend` and the related metadata-backend values. See [Iceberg REST Catalog Service > Metadata Backend Configuration](iceberg-rest-service.md#metadata-backend-configuration) for the backend options.

### Deploy in Ungoverned Mode

Ungoverned IRC runs as a standalone process that pulls catalog definitions from a Gravitino server over HTTP. Gravitino-orchestrated table maintenance works because catalogs remain Gravitino entities. Authentication and authorization fall outside Gravitino and become the operator's responsibility.

Prerequisite: a running Gravitino server reachable from the Kubernetes cluster, with a metalake created before this chart is deployed.

Set `icebergRest.catalogConfigProvider` to `dynamic-config-provider` and point `icebergRest.dynamicConfigProvider.uri` at the Gravitino server:

```console
helm upgrade --install gravitino-iceberg oci://registry-1.docker.io/apache/gravitino-iceberg-rest-server-helm --version <VERSION> \
  -n gravitino \
  --create-namespace \
  --set icebergRest.catalogConfigProvider=dynamic-config-provider \
  --set icebergRest.dynamicConfigProvider.uri=http://gravitino:8090 \
  --set icebergRest.dynamicConfigProvider.metalake=<metalake-name>
```

For Governed IRC, the recommended mode for production, use the main Gravitino chart with `auxService.names=iceberg-rest` enabled. See [Install Gravitino on Kubernetes](chart.md).

### Custom Configuration

To customize the deployment, use the `--set` flag to override specific values:

```console
helm upgrade --install gravitino-iceberg oci://registry-1.docker.io/apache/gravitino-iceberg-rest-server-helm --version <VERSION> \
  -n gravitino \
  --create-namespace \
  --set key1=val1,key2=val2,...
```

Alternatively, provide a custom values.yaml file:

```console
helm upgrade --install gravitino-iceberg oci://registry-1.docker.io/apache/gravitino-iceberg-rest-server-helm --version <VERSION> \
  -n gravitino \
  --create-namespace \
  -f /path/to/values.yaml
```

## Uninstall

```console
helm uninstall [RELEASE_NAME] -n [NAMESPACE]
```
