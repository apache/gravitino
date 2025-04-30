---
title: 'Catalog commands'
slug: /cli-catalog
license: 'This software is licensed under the Apache License version 2.'
---

## Catalog commands 

### List all catalogs in a metalake

```bash
gcli.sh catalog list
```

### Show a catalog details

```bash
gcli.sh catalog details --name my_catalog
```

### Show a catalog's audit information

```bash
gcli.sh catalog details --name my_catalog --audit
```

### Creating a catalog

The type of catalog to be created is specified by the `--provider` option.
Different catalogs require different properties.
For example, a Hive catalog requires a `metastore-uri` property.

#### Create a Hive catalog

```bash
gcli.sh catalog create --name hive --provider hive \
  --properties metastore.uris=thrift://hive-host:9083
```

#### Create an Iceberg catalog

```bash
gcli.sh catalog create --name iceberg --provider iceberg \
  --properties uri=thrift://hive-host:9083,catalog-backend=hive,warehouse=hdfs://hdfs-host:9000/user/iceberg/warehouse
```

#### Create a MySQL catalog

```bash
gcli.sh catalog create  -name mysql --provider mysql \
  --properties jdbc-url=jdbc:mysql://mysql-host:3306?useSSL=false,jdbc-user=user,jdbc-password=password,jdbc-driver=com.mysql.cj.jdbc.Driver
```

#### Create a Postgres catalog

```bash
gcli.sh catalog create  -name postgres --provider postgres \
  --properties jdbc-url=jdbc:postgresql://postgresql-host/mydb,jdbc-user=user,jdbc-password=password,jdbc-database=db,jdbc-driver=org.postgresql.Driver
```

#### Create a Kafka catalog

```bash
gcli.sh catalog create --name kafka --provider kafka \
  --properties bootstrap.servers=127.0.0.1:9092,127.0.0.2:9092
```

#### Create a Doris catalog

```bash
gcli.sh catalog create --name doris --provider doris \
  --properties jdbc-url=jdbc:mysql://localhost:9030,jdbc-driver=com.mysql.jdbc.Driver,jdbc-user=admin,jdbc-password=password
```

#### Create a Paimon catalog

```bash
gcli catalog create --name paimon --provider paimon \
  --properties catalog-backend=jdbc,uri=jdbc:mysql://127.0.0.1:3306/metastore_db,authentication.type=simple
```

#### Create a Hudi catalog

```bash
gcli.sh catalog create --name hudi --provider hudi \
  --properties catalog-backend=hms,uri=thrift://127.0.0.1:9083
```

#### Create an Oceanbase catalog

```bash
gcli.sh catalog create --name oceanbase --provider oceanbase \
  --properties jdbc-url=jdbc:mysql://localhost:2881,jdbc-driver=com.mysql.jdbc.Driver,jdbc-user=admin,jdbc-password=password
```

### Delete a catalog

```bash
gcli.sh catalog delete --name hive
```

### Rename a catalog

```bash
gcli.sh catalog update --name catalog_mysql --rename mysql
```

### Change a catalog comment

```bash
gcli.sh catalog update --name catalog_mysql --comment "new comment"
```

### Display a catalog's properties

```bash
gcli.sh catalog properties --name catalog_mysql
```

### Set a catalog's property

```bash
gcli.sh catalog set --name catalog_mysql --property test --value value
```

### Remove property from a catalog

```bash
gcli.sh catalog remove --name catalog_mysql --property test
```

### Enable a catalog

```bash
gcli.sh catalog update -m metalake_demo --name catalog --enable 
```

### Enable a catalog and its parent metalake

```bash
gcli.sh catalog update -m metalake_demo --name catalog --enable --all
```

### Disable a catalog

```bash
gcli.sh catalog update -m metalake_demo --name catalog --disable
```
