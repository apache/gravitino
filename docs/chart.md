---
title: "Helm Chart"
slug: /chart
keyword: chart
license: "This software is licensed under the Apache License version 2."
---

# Install Gravitino on Kubernetes

This Helm chart deploys Apache Gravitino on Kubernetes with customizable configurations.

## Prerequisites

- Kubernetes 1.29+
- Helm 3+

## Update Chart Dependency

The Gravitino Helm chart has not yet been officially released.   
To proceed, please clone the repository, navigate to the chart directory `/path/to/gravitino/dev/charts`, and execute the Helm dependency update command.

```console
helm dependency update [CHART]
```

## View Chart values

You can customize values.yaml parameters to override chart default settings. Additionally, Gravitino configurations in gravitino.conf can be modified through Helm values.yaml.

To display the default values of the Gravitino chart, run:

```console
helm show values [CHART]
```

## Install Helm Chart

```console
helm install [RELEASE_NAME] [CHART] [flags]
```

### Deploy with Default Configuration

Run the following command to deploy Gravitino using the default settings, specify container image versions using --set image.tag=x.y.z (replace x, y, z with the expected version numbers):

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace --set image.tag=<x.y.z>
```

### Deploy with Custom Configuration

To customize the deployment, use the --set flag to override specific values:

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace \
  --set key1=val1,key2=val2,...
```

Alternatively, you can provide a custom values.yaml file:

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace -f /path/to/chart/resources/scenarios/ci-values.yaml
```

_Note: \
/path/to/chart/resources/scenarios/ci-values.yaml is an example scenario to deploy._

### Deploying Gravitino with MySQL as the Storage Backend

To deploy both Gravitino and MySQL, where MySQL is used as the storage backend, enable the built-in MySQL instance:

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace \
  --set mysql.enabled=true
```

#### Disable Dynamic Storage Provisioning

By default, the MySQL PersistentVolumeClaim(PVC) storage class is local-path. To disable dynamic provisioning, set the storage class to "-":

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace \
  --set mysql.enabled=true \
  --set global.defaultStorageClass="-"
```

You must then manually create a PersistentVolume (PV).

### Deploy Gravitino using an existed MySQL Database

Ensure you have the following MySQL credentials ready: Username, Password, Database Name. When creating your database, we recommend calling it `gravitino`.

Before deploying Gravitino, initialize your existing MySQL instance and create the necessary tables required for Gravitino to function properly.

```console
mysql -h database-1.***.***.rds.amazonaws.com -P 3306 -u <YOUR-USERNAME> -p <YOUR-PASSWORD> < schema-0.*.0-mysql.sql
```

Use Helm to install or upgrade Gravitino, specifying the MySQL connection details.

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace \
  --set entity.jdbcUrl="jdbc:mysql://database-1.***.***.rds.amazonaws.com:3306/gravitino" \
  --set entity.jdbcDriver="com.mysql.cj.jdbc.Driver" \
  --set entity.jdbcUser="admin" \
  --set entity.jdbcPassword="admin123"
```

_Note: \
Replace database-1.***.***.rds.amazonaws.com with your actual MySQL host. \
Change admin and admin123 to your actual MySQL username and password. \
Ensure the target MySQL database (gravitino) exists before deployment._

## Uninstall Helm Chart

```console
helm uninstall [RELEASE_NAME] -n [NAMESPACE]
```