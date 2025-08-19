---
title: "Helm Chart"
slug: /chart
keyword: chart
license: "This software is licensed under the Apache License version 2."
---

# Gravitino Iceberg Rest Catalog Server Helm Chart

This Helm chart deploys Apache Gravitino Iceberg REST Catalog Server on Kubernetes with customizable configurations.

## Prerequisites

- Kubernetes 1.29+
- Helm 3+

## Update Chart Dependency

The Gravitino Iceberg REST Catalog Server Helm chart has not yet been officially released.   
To proceed, please clone the repository, navigate to the chart directory [charts](../dev/charts), and execute the Helm dependency update command.

```console
helm dependency update [CHART]
```

## View Chart values

You can customize values.yaml parameters to override chart default settings. Additionally, Gravitino Iceberg REST Catalog Server configurations in [gravitino-iceberg-rest-server.conf](../dev/charts/gravitino-iceberg-rest-server/resources/gravitino-iceberg-rest-server.conf) can be modified through Helm values.yaml.

To display the default values of the Gravitino chart, run:

```console
helm show values [CHART]
```

## Install Helm Chart

```console
helm install [RELEASE_NAME] [CHART] [flags]
```

### Deploy with Default Configuration

Run the following command to deploy Gravitino Iceberg REST Catalog Server using the default settings, specify container image versions using --set image.tag=x.y.z (replace x, y, z with the expected version numbers):

```console
helm upgrade --install gravitino ./gravitino-iceberg-rest-server \
  -n gravitino \
  --create-namespace \
  --set image.tag=<x.y.z> \
  --set replicas=2 \
  --set resources.requests.memory="4Gi" \
  --set resources.requests.cpu="2"
```

### Deploy with Custom Configuration

To customize the deployment, use the --set flag to override specific values:

```console
helm upgrade --install gravitino ./gravitino-iceberg-rest-server 
  -n gravitino \
  --create-namespace \
  --set key1=val1,key2=val2,...
```
Alternatively, you can provide a custom values.yaml file:

```console
helm upgrade --install gravitino ./gravitino-iceberg-rest-server 
  -n gravitino \
  --create-namespace \
  -f /path/to/values.yaml
```
_Note: \
The path '/path/to/values.yaml' refers to the actual path to the values.yaml file._

## Uninstall Helm Chart

```console
helm uninstall [RELEASE_NAME] -n [NAMESPACE]
```