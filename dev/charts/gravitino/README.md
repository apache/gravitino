<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Gravitino

Apache Gravitino is a high-performance, geo-distributed, and federated metadata lake. It manages the
metadata directly in
different sources, types, and regions. It also provides users with unified metadata access
for data and AI assets.

**Homepage:** <https://gravitino.apache.org/>

## Prerequisites

- Kubernetes 1.30+
- Helm 3+

## Maintainers

| Name             | Email                    | Url                          |
|------------------|--------------------------|------------------------------|
| Apache Gravitino | dev@gravitino.apache.org | https://gravitino.apache.org |

## Source Code

* <https://github.com/apache/gravitino>
* <https://github.com/apache/gravitino-playground>

## Update Chart Dependency

If the chart has not been released yet, navigate to the chart directory and update its dependencies:

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

Run the following command to deploy Gravitino using the default settings:

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace
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

## Package Helm Chart

```console
helm package [CHART_PATH]
```