---
title: "Install Gravitino on Kubernetes"
slug: "/chart"
keyword: "chart"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

This Helm chart deploys Apache Gravitino on Kubernetes with customizable configurations.

## Prerequisites

- Kubernetes 1.29+
- Helm 3+

## Installation

### Install from OCI Registry (Recommended for Released Versions)

Pull the chart from Docker Hub OCI registry:

```console
helm pull oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION>
```

Or install directly:

```console
helm upgrade --install gravitino oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION> -n <NAMESPACE> --create-namespace
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
helm upgrade --install gravitino ./gravitino -n <NAMESPACE> --create-namespace
```

## View Chart Values

Customize values.yaml parameters to override chart default settings. Additionally, Gravitino configurations in gravitino.conf can be modified through Helm values.yaml.

To display the default values of the Gravitino chart, run:

```console
helm show values oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION>
```

## Install Helm Chart

```console
helm upgrade --install [RELEASE_NAME] oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION> [flags]
```

### Deploy with Default Configuration

Run the following command to deploy Gravitino using the default settings:

```console
helm upgrade --install gravitino oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION> -n <NAMESPACE> --create-namespace
```

### Deploy with Custom Configuration

To customize the deployment, use the --set flag to override specific values:

```console
helm upgrade --install gravitino oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION> \
  -n <NAMESPACE> --create-namespace \
  --set key1=val1,key2=val2,...
```

Alternatively, you can provide a custom values.yaml file:

```console
helm upgrade --install gravitino oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION> \
  -n <NAMESPACE> --create-namespace \
  -f /path/to/values.yaml
```

### Deploy Gravitino with MySQL as the Storage Backend

To deploy both Gravitino and MySQL, where MySQL is used as the storage backend, enable the built-in MySQL instance:

```console
helm upgrade --install gravitino oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION> \
  -n <NAMESPACE> --create-namespace \
  --set mysql.enabled=true
```

#### Disable Dynamic Storage Provisioning

By default, the MySQL PersistentVolumeClaim(PVC) storage class is local-path. To disable dynamic provisioning, set the storage class to "-":

```console
helm upgrade --install gravitino oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION> \
  -n <NAMESPACE> --create-namespace \
  --set mysql.enabled=true \
  --set global.defaultStorageClass="-"
```

Then manually create a PersistentVolume (PV).

### Deploy Gravitino Using an Existed MySQL Database

Ensure you have the following MySQL credentials ready: Username, Password, Database Name. When creating your database, we recommend calling it `gravitino`.

Before deploying Gravitino, initialize your existing MySQL instance and create the necessary tables required for Gravitino to function properly.

```console
mysql -h database-1.***.***.rds.amazonaws.com -P 3306 -u <YOUR-USERNAME> -p <YOUR-PASSWORD> < schema-0.*.0-mysql.sql
```

Use Helm to install or upgrade Gravitino, specifying the MySQL connection details.

```console
helm upgrade --install gravitino oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION> \
  -n <NAMESPACE> --create-namespace \
  --set entity.jdbcUrl="jdbc:mysql://database-1.***.***.rds.amazonaws.com:3306/gravitino" \
  --set entity.jdbcDriver="com.mysql.cj.jdbc.Driver" \
  --set entity.jdbcUser="admin" \
  --set entity.jdbcPassword="admin123"
```

_Note: \
Replace database-1.***.***.rds.amazonaws.com with your actual MySQL host. \
Change admin and admin123 to your actual MySQL username and password. \
Ensure the target MySQL database (gravitino) exists before deployment._

### Deploy Gravitino with GCS as Object Store

If your catalog uses Google Cloud Storage (GCS) as the object store, you need to set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable in the container and mount the service account key file.

1. Create a Kubernetes secret from your GCS service account key:

```console
kubectl create secret generic gcs-key --from-file=key.json=/path/to/your-service-account-key.json -n <NAMESPACE>
```

2. Deploy Gravitino with GCS configuration in your custom `values.yaml`:

```yaml
env:
  - name: GRAVITINO_MEM
    value: "-Xms1024m -Xmx1024m -XX:MaxMetaspaceSize=512m"
  - name: GOOGLE_APPLICATION_CREDENTIALS
    value: /etc/gcs/key.json

extraVolumes:
  - name: gravitino-log
    emptyDir: {}
  - name: gcs-key
    secret:
      secretName: gcs-key

extraVolumeMounts:
  - name: gravitino-log
    mountPath: /opt/gravitino/logs
  - name: gcs-key
    mountPath: /etc/gcs
    readOnly: true
```

```console
helm upgrade --install gravitino oci://registry-1.docker.io/apache/gravitino-helm --version <VERSION> \
  -n <NAMESPACE> --create-namespace \
  -f /path/to/values.yaml
```

## Uninstall Helm Chart

```console
helm uninstall [RELEASE_NAME] -n <NAMESPACE>
```
