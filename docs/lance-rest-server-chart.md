---
title: "Install Lance REST Server on Kubernetes"
slug: /lance-rest-server-chart
keyword: 
  - Lance REST Server Helm Chart
license: "This software is licensed under the Apache License version 2."
---

# Install Lance REST Server on Kubernetes

This Helm chart deploys Apache Gravitino Lance REST Server on Kubernetes with customizable configurations.

## Prerequisites

- Kubernetes 1.29+
- Helm 3+

## Installation

### Install from OCI Registry (Recommended for Released Versions)

Pull the chart from Docker Hub OCI registry:

```console
helm pull oci://registry-1.docker.io/apache/gravitino-lance-rest-server-helm --version <VERSION>
```

Or install directly:

```console
helm upgrade --install gravitino-lance oci://registry-1.docker.io/apache/gravitino-lance-rest-server-helm --version <VERSION> -n gravitino --create-namespace
```

### Install from Local Repository (For Development or Unreleased Versions)

Clone the repository and navigate to the chart directory:

```console
git clone https://github.com/apache/gravitino.git
cd gravitino/dev/charts
```

Update chart dependencies:

```console
helm dependency update gravitino-lance-rest-server
```

Install the chart:

```console
helm upgrade --install gravitino-lance ./gravitino-lance-rest-server -n gravitino --create-namespace
```

## View Chart Values

You can customize values.yaml parameters to override chart default settings. Additionally, Gravitino Lance REST Server configurations in [gravitino-lance-rest-server.conf](../dev/charts/gravitino-lance-rest-server/resources/gravitino-lance-rest-server.conf) can be modified through Helm values.yaml.

To display the default values of the chart, run:

```console
helm show values oci://registry-1.docker.io/apache/gravitino-lance-rest-server-helm --version <VERSION>
```

## Install Helm Chart

```console
helm upgrade --install [RELEASE_NAME] oci://registry-1.docker.io/apache/gravitino-lance-rest-server-helm --version <VERSION> [flags]
```

### Deploy with Default Configuration

Run the following command to deploy Gravitino Lance REST Server using the default settings:

```console
helm upgrade --install gravitino-lance oci://registry-1.docker.io/apache/gravitino-lance-rest-server-helm --version <VERSION> \
  -n gravitino \
  --create-namespace \
  --set lanceRest.gravitinoUri=http://gravitino:8090 \
  --set lanceRest.gravitinoMetalake=your-metalake \
  --set replicas=2 \
  --set resources.requests.memory="4Gi" \
  --set resources.requests.cpu="2"
```

### Deploy with Custom Configuration

To customize the deployment, use the --set flag to override specific values:

```console
helm upgrade --install gravitino-lance oci://registry-1.docker.io/apache/gravitino-lance-rest-server-helm --version <VERSION> \
  -n gravitino \
  --create-namespace \
  --set key1=val1,key2=val2,...
```

Alternatively, you can provide a custom values.yaml file:

```console
helm upgrade --install gravitino-lance oci://registry-1.docker.io/apache/gravitino-lance-rest-server-helm --version <VERSION> \
  -n gravitino \
  --create-namespace \
  -f /path/to/values.yaml
```

## Configuration Notes

### Gravitino Backend Configuration

Make sure to configure the Gravitino backend connection properly:

```yaml
lanceRest:
  gravitinoUri: http://your-gravitino-server:8090
  gravitinoMetalake: your-metalake-name
```

The Lance REST Server requires a running Gravitino instance to function. Ensure:
1. The Gravitino server is accessible from the Lance REST Server pods
2. The metalake specified in `gravitinoMetalake` exists in Gravitino
3. Network policies allow communication between Lance REST and Gravitino

## Uninstall Helm Chart

```console
helm uninstall [RELEASE_NAME] -n [NAMESPACE]
```
