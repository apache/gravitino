---
title: "Install Iceberg REST Catalog Server on Kubernetes"
slug: "/iceberg-rest-catalog-chart"
keyword:
  - Iceberg REST Helm Chart
license: "This software is licensed under the Apache License version 2."
---

## Introduction

This Helm chart deploys the Apache Gravitino Iceberg REST catalog server on Kubernetes with customizable configurations.

## Prerequisites

- Kubernetes 1.29+
- Helm 3+

## Installation

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

## View Chart Values

Override chart defaults by customizing parameters in `values.yaml`. Configuration in [gravitino-iceberg-rest-server.conf](../dev/charts/gravitino-iceberg-rest-server/resources/gravitino-iceberg-rest-server.conf) can also be modified through Helm `values.yaml`.

To display the default values for the chart, run:

```console
helm show values oci://registry-1.docker.io/apache/gravitino-iceberg-rest-server-helm --version <VERSION>
```

## Install Helm Chart

```console
helm upgrade --install [RELEASE_NAME] oci://registry-1.docker.io/apache/gravitino-iceberg-rest-server-helm --version <VERSION> [flags]
```

### Deploy with Default Configuration

Deploy the Gravitino Iceberg REST catalog server with the default settings:

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
