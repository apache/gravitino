---
title: "Install Iceberg Rest catalog server on Kubernetes"
slug: /iceberg-rest-catalog-chart
keyword:
  - Iceberg REST Helm Chart
license: "This software is licensed under the Apache License version 2."
---

# Install Iceberg Rest catalog server on Kubernetes

This Helm chart deploys Apache Gravitino Iceberg REST Catalog Server on Kubernetes with customizable configurations.

## Prerequisites

- Kubernetes 1.29+
- Helm 3+

## Installation

Pull the chart from Docker Hub OCI registry:

```console
helm pull oci://registry-1.docker.io/apache/gravitino-iceberg-rest-server-helm --version <VERSION>
```

Or install directly:

```console
helm upgrade --install gravitino-iceberg oci://registry-1.docker.io/apache/gravitino-iceberg-rest-server-helm --version <VERSION> -n gravitino --create-namespace
```

## View Chart Values

You can customize values.yaml parameters to override chart default settings. Additionally, Gravitino Iceberg REST Catalog Server configurations in [gravitino-iceberg-rest-server.conf](../dev/charts/gravitino-iceberg-rest-server/resources/gravitino-iceberg-rest-server.conf) can be modified through Helm values.yaml.

To display the default values of the chart, run:

```console
helm show values oci://registry-1.docker.io/apache/gravitino-iceberg-rest-server-helm --version <VERSION>
```

## Install Helm Chart

```console
helm upgrade --install [RELEASE_NAME] oci://registry-1.docker.io/apache/gravitino-iceberg-rest-server-helm --version <VERSION> [flags]
```

### Deploy with Default Configuration

Run the following command to deploy Gravitino Iceberg REST Catalog Server using the default settings:

```console
helm upgrade --install gravitino-iceberg oci://registry-1.docker.io/apache/gravitino-iceberg-rest-server-helm --version <VERSION> \
  -n gravitino \
  --create-namespace \
  --set replicas=2 \
  --set resources.requests.memory="4Gi" \
  --set resources.requests.cpu="2"
```

### Deploy with Custom Configuration

To customize the deployment, use the --set flag to override specific values:

```console
helm upgrade --install gravitino-iceberg oci://registry-1.docker.io/apache/gravitino-iceberg-rest-server-helm --version <VERSION> \
  -n gravitino \
  --create-namespace \
  --set key1=val1,key2=val2,...
```

Alternatively, you can provide a custom values.yaml file:

```console
helm upgrade --install gravitino-iceberg oci://registry-1.docker.io/apache/gravitino-iceberg-rest-server-helm --version <VERSION> \
  -n gravitino \
  --create-namespace \
  -f /path/to/values.yaml
```

## Uninstall Helm Chart

```console
helm uninstall [RELEASE_NAME] -n [NAMESPACE]
```
