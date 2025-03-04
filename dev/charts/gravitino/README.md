# gravitino

Gravitino is a high-performance, geo-distributed, and federated metadata lake. It manages the
metadata directly in
different sources, types, and regions. It also provides users with unified metadata access
for data and AI assets.

**Homepage:** <https://gravitino.apache.org/>

## Maintainers

| Name   | Email                 | Url |
|--------|-----------------------|-----|
| danhua | danhua@datastrato.com |     |

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