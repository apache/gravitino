---
title: "Install Gravitino on Kubernetes"
slug: "/chart"
keyword: "chart"
license: "This software is licensed under the Apache License version 2."
---

Deploy Apache Gravitino on Kubernetes with the Apache Gravitino Helm chart. Values in `values.yaml` and overrides in `gravitino.conf` are both customizable through Helm.

## Prerequisites

- Kubernetes 1.29+
- Helm 3+

## Sources

### Install from OCI Registry (Recommended for Released Versions)

Pull the chart from Docker Hub OCI registry:

```console
helm pull oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION>
```

Or install directly:

```console
helm upgrade --install gravitino oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION> -n gravitino --create-namespace
```

### Install from Local Repository (for Development or Unreleased Versions)

Clone the repository and navigate to the chart directory:

```console
git clone https://github.com/apache/gravitino.git
cd gravitino/dev/charts
```

Update chart dependencies:

```console
helm dependency update gravitino
```

Install the chart:

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace
```

## Chart Values

The chart ships sensible defaults in its `values.yaml`. To deploy with different settings, supply your own values file or pass `--set` flags at install time; Helm merges your overrides on top of the chart defaults using a deep merge, so any settings you do not mention keep their default values.

### Inspecting the Defaults

To see the full set of values the chart understands, including their defaults and inline comments:

```console
helm show values oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION>
```

The output is the chart's `values.yaml` in full. Each setting includes a comment describing its purpose. For property-by-property explanations of how Gravitino server configuration maps to chart values, see [Gravitino Server Configuration](./gravitino-server-config.md).

### Customizing With a Values File

Write your overrides into a YAML file containing only the keys you want to change. For example, `my-values.yaml`:

```yaml
cache:
  maxEntries: 100000
  enableStats: true

audit:
  enabled: true
```

Apply the file at install time with the `-f` flag:

```console
helm upgrade --install gravitino oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION> \
  -n gravitino --create-namespace \
  -f my-values.yaml
```

Helm reads the chart's built-in `values.yaml` (defaults), overlays your file on top, and templates the resulting configuration. Settings not mentioned in your file keep their default values.

### One-Off Overrides With --set

For quick overrides without writing a file, use `--set`:

```console
helm upgrade --install gravitino oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION> \
  -n gravitino --create-namespace \
  --set audit.enabled=true \
  --set cache.maxEntries=100000
```

When both `-f` and `--set` are used, `--set` takes precedence over `-f`, and `-f` takes precedence over the chart's defaults.

### Example Scenario Files

The chart distribution includes example values files in `resources/scenarios/` for common deployment scenarios. These are starting templates: copy one out, customize for your environment, then install with `-f`. See the dev and production scenarios under [Deployment](#deployment) below.

## Deployment

### Deploy with Default Configuration

Run the following command to deploy Gravitino using the default settings:

```console
helm upgrade --install gravitino oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION> -n gravitino --create-namespace
```

### Deploy With the Dev Scenario

The chart ships a `dev-values.yaml` scenario file at `resources/scenarios/dev-values.yaml`. The dev scenario opts in to the dynamic config provider for the Iceberg REST server so the IRC server federates a local Gravitino metalake named `test`. Other settings stay at chart defaults (embedded H2 metadata backend, simple authentication, no persistence), which are appropriate for local development.

Extract the chart to access the scenario file:

```console
helm pull oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION> --untar
```

The dev scenario applies as-is, no customization required:

```console
helm upgrade --install gravitino oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION> \
  -n gravitino --create-namespace \
  -f gravitino/resources/scenarios/dev-values.yaml
```

For the equivalent configuration applied directly to `gravitino.conf` (for the binary install path), see [Development Configuration](./gravitino-server-config.md#development).

### Deploy With the Production Scenario

The chart ships a `prod-values.yaml` scenario file at `resources/scenarios/prod-values.yaml`. The production scenario configures externally managed MySQL as the metadata backend, larger cache and tree-lock limits, audit logging, the Iceberg REST server with the dynamic config provider, and OAuth 2.0 OIDC authentication with JWKS-based token validation.

The file contains placeholder values that must be filled in for your environment before applying.

Extract the chart and copy the scenario file:

```console
helm pull oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION> --untar
cp gravitino/resources/scenarios/prod-values.yaml my-prod-values.yaml
```

Edit `my-prod-values.yaml` to fill in the placeholders:

- `<your-mysql-host>`, `<your-database>`, `<your-mysql-user>`, `<your-mysql-password>` for the metadata backend
- `<your-tenant-id>`, `<your-app-client-id>`, `<your-app-client-id-or-api-identifier>` for OIDC authentication

Before installing, initialize the MySQL metadata backend. See [How to Use Relational Backend Storage](./how-to-use-relational-backend-storage.md) for setup steps and SQL scripts.

Apply the customized values file:

```console
helm upgrade --install gravitino oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION> \
  -n gravitino --create-namespace \
  -f my-prod-values.yaml
```

For the equivalent configuration applied directly to `gravitino.conf`, see [Production Configuration](./gravitino-server-config.md#production).

The MySQL password and OAuth client secret are plaintext placeholders in `prod-values.yaml`. For production deployments, source these from Kubernetes Secrets rather than embedding them in your values file; chart-native Kubernetes Secret references are tracked in the chart's enterprise readiness work and will be documented when available.

### Deploy Gravitino with MySQL as the Storage Backend

To deploy both Gravitino and MySQL, where MySQL is used as the storage backend, enable the built-in MySQL instance:

```console
helm upgrade --install gravitino oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION> \
  -n gravitino --create-namespace \
  --set mysql.enabled=true
```

#### Disable Dynamic Storage Provisioning

By default, the MySQL PersistentVolumeClaim(PVC) storage class is local-path. To disable dynamic provisioning, set the storage class to "-":

```console
helm upgrade --install gravitino oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION> \
  -n gravitino --create-namespace \
  --set mysql.enabled=true \
  --set global.defaultStorageClass="-"
```

Then manually create a PersistentVolume (PV).

## Uninstall

```console
helm uninstall [RELEASE_NAME] -n [NAMESPACE]
```
