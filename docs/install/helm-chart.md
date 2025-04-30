---
title: Helm Chart
slug: /chart
keyword: chart
license: "This software is licensed under the Apache License version 2."
---

# Apache Gravitino Helm Chart

This Helm chart deploys Apache Gravitino on Kubernetes with customizable configurations.

## Prerequisites

- Kubernetes 1.29+
- Helm 3+

## Update Chart Dependency

If the chart has not been released yet, navigate to the `chart` directory and update its dependencies:

```console
helm dependency update [CHART]
```

## Package Helm Chart

```shell
helm package [CHART_PATH]
```

## View Chart values

You can customize values.yaml parameters to override chart default settings.
Additionally, Gravitino configurations in `gravitino.conf` can be modified through Helm `values.yaml`.

To display the default values of the Gravitino chart, run:

```shell
helm show values [CHART]
```

## Install Helm Chart

```shell
helm install [RELEASE_NAME] [CHART] [flags]
```

### Deploy with default configuration

Run the following command to deploy Gravitino using the default settings:

```shell
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace
```

### Deploy with custom configuration

To customize the deployment, use the `--set` flag to override specific values:

```shell
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace \
  --set key1=val1,key2=val2,...
```

Alternatively, you can provide a custom `values.yaml` file:

```shell
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace \
  -f /path/to/chart/resources/scenarios/ci-values.yaml
```

:::note
`/path/to/chart/resources/scenarios/ci-values.yaml` is an example deploy scenario.
:::

### Deploying Gravitino with MySQL as the Storage Backend

To deploy Gravitino with MySQL as the storage backend, enable the built-in MySQL instance:

```shell
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace \
  --set mysql.enabled=true
```

#### Disable Dynamic Storage Provisioning

By default, the MySQL PersistentVolumeClaim(PVC) storage class is local-path.
To disable dynamic provisioning, set the storage class to "-":

```shell
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace \
  --set mysql.enabled=true \
  --set global.defaultStorageClass="-"
```

You must then manually create a PersistentVolume (PV).

### Deploy Gravitino using an existed MySQL Database

Ensure you have the following MySQL credentials ready: Username, Password, Database Name.
When creating your database, we recommend calling it `gravitino`.

Before deploying Gravitino, initialize your existing MySQL instance and
create the necessary tables required for Gravitino to function properly.

```shell
mysql -h database-1.***.***.rds.amazonaws.com -P 3306 \
  -u <YOUR-USERNAME> -p <YOUR-PASSWORD> < schema-0.*.0-mysql.sql
```

Use Helm to install or upgrade Gravitino, specifying the MySQL connection details.

```shell
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace \
  --set entity.jdbcUrl="jdbc:mysql://database-1.***.***.rds.amazonaws.com:3306/gravitino" \
  --set entity.jdbcDriver="com.mysql.cj.jdbc.Driver" \
  --set entity.jdbcUser="admin" \
  --set entity.jdbcPassword="admin123"
```

:::note
Replace `database-1.***.***.rds.amazonaws.com` with your actual MySQL host.
Change `admin` and `admin123` to your actual MySQL username and password.
Ensure the target MySQL database (gravitino) exists before deployment.
:::

## Uninstall Helm Chart

```shell
helm uninstall [RELEASE_NAME] -n [NAMESPACE]
```


