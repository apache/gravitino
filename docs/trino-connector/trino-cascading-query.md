---
title: "Apache Gravitino connector Trino cascading query"
slug: /trino-connector/trino-cascading-query
keyword: gravitino connector trino
license: "This software is licensed under the Apache License version 2."
---

# Background
With `Apache Gravitino Trino connector` and `Gravitino Trino cascading connector`, you can implement cascading queries in Trino.
These connectors allow you to treat other Trino clusters as data sources for the current Trino cluster,
enabling queries across catalogs in different Trino clusters.

This mechanism prioritizes executing queries in the Trino cluster located in the same region as the data,
based on the data distribution in the catalogs. By doing so, it significantly reduces the amount of data
transferred over the network, addressing the performance issues commonly found in traditional federated query engines
where large volumes of data need to be transmitted across networks.

# Deploying Trino

## Deploying Trino

To setup the Apache Gravitino Trino cascading query environment, you should first deploy tow Trino environment,
and then install the Apache Gravitino Trino connector plugin and Gravitino Trino cascading connector plugin into Trino.
Please refer to the [Deploying Trino documentation](installation.md) and do the following steps:

1. [Download](https://github.com/apache/gravitino/releases) the Gravitino Trino connector tarball and unpack it.
   The tarball contains a single top-level directory `gravitino-trino-connector-<version>`, and rename the directory to `gravitino`.
2. [Download](https://github.com/datastrato/trino-cascading-connector/releases) the Gravitino Trino cascading connector tarball and unpack it.
   The tarball contains a single top-level directory `gravitino-trino-cascading-connector-<version>`, and rename the directory to `trino`.
3. Copy the two connector directories to the Trino's plugin directory.
   Normally, the directory location is `Trino-server-<version>/plugin`, and the directory contains other catalogs used by Trino.

Ensure that the `plugin` directory contains `gravitino` and 
`trino` subdirectories. Two Trino clusters need to be deployed on machines with hostnames `c1-trino` and `c2-trino`. 
Ensure network connectivity between these machines. 

## Deploying Trino in Containers

Download the Gravitino Trino connector tarball and Gravitino Trino cascading connector tarball and unpack them.
You can see the connector directory `gravitino-trino-connector-<version>` and `gravitino-trino-cascading-connector-<version>`
after unpacking. 

To start Trino on the host `c1-trino` and mount the plugins. execute the following command:

```bash
docker run --name c1-trino -d -p 8080:8080 <image-name> -v `gravitino-trino-connector-<version>`:/usr/lib/trino/plugin/gravitino   
-v `gravitino-trino-cascading-connector-<version>`:/usr/lib/trino/plugin/trino
```

To start Trino on the host `c2-trino` and mount the plugins. execute the following command:

```bash
docker run --name c1-trino -d -p 8080:8080 <image-name> -v `gravitino-trino-connector-<version>`:/usr/lib/trino/plugin/gravitino   
-v `gravitino-trino-cascading-connector-<version>`:/usr/lib/trino/plugin/trino
```

After starting the Trino container, the configuration directory is located at `/etc/trino`, 
Ensure that the Trino containers on `c1-trino` and `c2-trino` can communicate with each other over the network.

## Configuring Trino

For detailed instructions on configuring Trino, 
please refer to the (documentation)[https://trino.io/docs/current/installation/deployment.html#configuring-trino]. 
After basic configuration, create a `gravitino.properties` configuration file in the `etc/catalog` directory 
on the `c1-trino` host to configure the Gravitino connector:

```text
connector.name = gravitino
gravitino.uri = http://GRAVITINO_HOST_IP:GRAVITINO_HOST_PORT
gravitino.metalake = GRAVITINO_METALAKE_NAME
gravitino.cloud.region-code=c1
```

The `gravitino.cloud.region-code=c1` indicates that the `c1-trino` host belongs to the `c1` region, 
and catalogs in the `c1` region will execute queries on `c1-trino`. 
For catalogs in the `c2` region, queries will be delegated to the `c2-trino` host. 

Similarly, create a `gravitino.properties` file in the `etc/catalog` directory on the `c2-trino` host:

```test
connector.name = gravitino
gravitino.uri = http://GRAVITINO_HOST_IP:GRAVITINO_HOST_PORT
gravitino.metalake = GRAVITINO_METALAKE_NAME
gravitino.cloud.region-code=c2
```

The `gravitino.cloud.region-code=c2` indicates that the `c2-trino` host belongs to the `c2` region, 
and only queries in catalogs in the `c2` region will execute on `c2-trino`. 

Ensure that the `gravitino.uri` configuration on `c1-trino` and `c2-trino` points to the same Gravitino server and 
that the service is running and connected properly. Restart Trino after configuration.

## Creating Catalogs

To verify federated queries, create catalogs. 
Below is an example using the `gt_mysql` catalog in the `c2` region, configured for federated querying from `c1-trino`. 
Execute the following command in the `c1-trino` CLI to create the catalog:

```sql
CALL gravitino.system.create_catalog(
    'gt_mysql',
    'jdbc-mysql',
    MAP(
        ARRAY['jdbc-url', 'jdbc-user', 'jdbc-password', 'jdbc-driver', 'cloud.region-code', 'cloud.trino.connection-url', 'cloud.trino.connection-user', 'cloud.trino.connection-password'],
        ARRAY['${mysql_uri}/?useSSL=false', 'trino', 'ds123', 'com.mysql.cj.jdbc.Driver', 'c2', 'jdbc:trino://c2-trino:8080', 'admin', '']
    )
);
```

Where:
- `cloud.region-code` specifies that the `gt_mysql` catalog is in the `c2` region.
- `cloud.trino.connection-url` specifies the Trino JDBC connection URL for the Trino in `c2` region.
- `cloud.trino.connection-user` specifies the Trino JDBC user for the Trino in `c2` region.
- `cloud.trino.connection-password` specifies the Trino JDBC user password for the Trino in `c2` region.

Using this configuration, `c1-trino` can access `c2-trino` through the specified Trino JDBC connection information. 
After successfully creating the catalog, use the Trino CLI on `c1-trino` and `c2-trino` to view the catalogs:

```sql
SHOW CATALOGS;
```

## Adding Data

After creating the catalog, add data to verify queries. 
Since the Trino federated query connector does not support data writing, use the `c2-trino` CLI to execute the following commands:

```sql
CREATE SCHEMA gt_mysql.gt_db1;

CREATE TABLE gt_mysql.gt_db1.tb01 (
    name VARCHAR(255),
    salary INT
);

INSERT INTO gt_mysql.gt_db1.tb01(name, salary) VALUES ('sam', 11);
INSERT INTO gt_mysql.gt_db1.tb01(name, salary) VALUES ('jerry', 13);
INSERT INTO gt_mysql.gt_db1.tb01(name, salary) VALUES ('bob', 14), ('tom', 12);
```

## Query Verification

After successfully inserting the data, execute the following query using the `c1-trino` CLI:

```sql
SELECT * FROM gt_mysql.gt_db1.tb01 ORDER BY name;
```

Verify the query results using the `c2-trino` CLI:

```sql
SELECT * FROM gt_mysql.gt_db1.tb01 ORDER BY name;
```

The `c2-trino` CLI output is the same as `c1-trino`