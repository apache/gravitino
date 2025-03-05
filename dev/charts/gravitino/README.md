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
# gravitino

Gravitino is a high-performance, geo-distributed, and federated metadata lake. It manages the
metadata directly in
different sources, types, and regions. It also provides users with unified metadata access
for data and AI assets.

**Homepage:** <https://gravitino.apache.org/>

## Source Code

* <https://github.com/apache/gravitino>
* <https://github.com/apache/gravitino-playground>

## Values

| Key                 | Default                   | Description                                                           |
|---------------------|---------------------------|-----------------------------------------------------------------------|
| image.repository    | `"apache/gravitino"`      |                                                                       |
| image.tag           | `"0.8.0-incubating"`      |                                                                       |
| image.pullPolicy    | `"IfNotPresent"`          |                                                                       |
| image.pullSecrets   | `[]       `               | Optionally specify secrets for pulling images from a private registry |                            
| mysql.enabled       | `false`                   | Flag to enable MySQL as the storage backend for Gravitino             |
| entity.jdbcUrl      | `"jdbc:h2"`               | The JDBC URL for the database                                         |
| entity.jdbcDriver   | `"org.h2.Driver"`         | The JDBC driver class name                                            |
| entity.jdbcUser     | `"gravitino"`             | The username for the JDBC connection                                  |
| entity.jdbcPassword | `"gravitino"`             | The password for the JDBC connection                                  |
| env                 | `HADOOP_USER_NAME: hdfs ` | Environment variables to pass to the container                        |
| resources           | `{}            `          | esource requests and limits for the container                         |

## Deploy gravitino to your cluster

### Update chart dependency

```bash
cd gravitino
helm dependency update
```

### Deploy with default config

```bash
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace
```

### Deploy with custom config

```bash
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace --set "key1=val1,key2=val2,..."
```

## Deploy gravitino and MySQL, MySQL is the gravitino storage backend

```bash
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace --set mysql.enabled=true
```

To disable dynamic provisioning (The default STORAGECLASS is local-path)

```bash
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace --set mysql.enabled=true --set global.defaultStorageClass="-"
```

## Deploy gravitino, use the existed MySQL as the gravitino storage backend

```bash
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace --set entity.jdbcUrl="jdbc:mysql://database-1.***.***.rds.amazonaws.com:3306/gravitino" --set entity.jdbcDriver="com.mysql.cj.jdbc.Driver" --set entity.jdbcUser=admin --set entity.jdbcPassword=admin123
```

## Others

To init the existed MySQL, run the following command:

```bash
mysql -h database-1.***.***.rds.amazonaws.com -P 3306 -u <YOUR-USERNAME> -p <YOUR-PASSWORD> < schema-0.8.0-mysql.sql
```

To see the "gravitino.conf", run the following command:

```bash
kubectl get cm gravitino -n gravitino -o json | jq -r '.data["gravitino.conf"]'
```

To uninstall the gravitino, run the following command:

```bash
helm uninstall gravitino -n gravitino
```

To package chart

```bash
helm package gravitino
```