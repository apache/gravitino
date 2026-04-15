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

## Update Chart Dependency

The Gravitino Lance REST Server Helm chart has not yet been officially released.   
To proceed, please clone the repository, navigate to the chart directory [charts](../dev/charts), and execute the Helm dependency update command.

```console
helm dependency update [CHART]
```

## View Chart values

You can customize values.yaml parameters to override chart default settings. Additionally, Gravitino Lance REST Server configurations in [gravitino-lance-rest-server.conf](../dev/charts/gravitino-lance-rest-server/resources/gravitino-lance-rest-server.conf) can be modified through Helm values.yaml.

To display the default values of the chart, run:

```console
helm show values [CHART]
```

## Install Helm Chart

```console
helm install [RELEASE_NAME] [CHART] [flags]
```

### Deploy with Default Configuration

Run the following command to deploy Gravitino Lance REST Server using the default settings, specifying the container image version using `--set image.tag=<version>` (replace `<version>` with the desired image tag):

```console
helm upgrade --install gravitino ./gravitino-lance-rest-server \
  -n gravitino \
  --create-namespace \
  --set image.tag=<version> \
  --set replicas=2 \
  --set resources.requests.memory="4Gi" \
  --set resources.requests.cpu="2"
```

### Deploy with Custom Configuration

To customize the deployment, use the --set flag to override specific values:

```console
helm upgrade --install gravitino ./gravitino-lance-rest-server \
  -n gravitino \
  --create-namespace \
  --set key1=val1,key2=val2,...
```
Alternatively, you can provide a custom values.yaml file:

```console
helm upgrade --install gravitino ./gravitino-lance-rest-server \
  -n gravitino \
  --create-namespace \
  -f /path/to/values.yaml
```
_Note: \
The path '/path/to/values.yaml' refers to the actual path to the values.yaml file._

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
