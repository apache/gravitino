---
title: "Apache Gravitino connector Trino cascading query"
slug: /trino-connector/trino-cascading-query
keyword: gravitino connector trino
license: "This software is licensed under the Apache License version 2."
---

# Background
With the `Apache Gravitino Trino connector` and the [`Gravitino Trino cascading connector`](https://github.com/datastrato/trino-cascading-connector), you can implement cascading queries in Trino.
These connectors allow you to treat other Trino clusters as data sources for the current Trino cluster,
enabling queries across catalogs in different Trino clusters.

This mechanism prioritizes executing queries in the Trino cluster located in the same region as the data,
based on the data distribution in the catalogs. By doing so, it significantly reduces the amount of data
transferred over the network, addressing the performance issues commonly found in traditional federated query engines
where large volumes of data need to be transmitted across networks.

# Deploying Trino

## Deploying Trino

To set up the Trino cascading query environment, you should first deploy at least two Trino environments.
Next, install the `Apache Gravitino Trino connector` plugin and `Gravitino Trino cascading connector` plugin into Trino.
For detailed steps, please refer to the [Deploying Trino documentation](installation.md).

Follow these steps:

1. [Download](https://github.com/apache/gravitino/releases) the `Apache Gravitino Trino connector` tarball and unpack it.
   The tarball contains a single top-level directory named `gravitino-trino-connector-<version>`. Rename this directory to `gravitino`.
2. [Download](https://github.com/datastrato/trino-cascading-connector/releases) the `Gravitino Trino cascading connector` tarball and unpack it.
   This tarball also contains a single top-level directory named `gravitino-trino-cascading-connector-<version>`. Rename this directory to `trino`.
3. Copy both connector directories to Trino's plugin directory.
   Typically, this directory is located at `Trino-server-<version>/plugin` and contains other catalogs used by Trino.

Ensure that the `plugin` directory includes the `gravitino` and `trino` subdirectories.
Verify the network connectivity between the machines hosting the two Trino clusters, identified as `c1-trino` and `c2-trino`.


## Deploying Trino in Containers

Download the `Apache Gravitino Trino connector` tarball and `Gravitino Trino cascading connector` tarball, then unpack them.
After unpacking, you will find the directories named `gravitino-trino-connector-<version>`
and `gravitino-trino-cascading-connector-<version>`.

To start Trino on the host `c1-trino` and mount the plugins, execute the following command:

```bash
docker run --name c1-trino -d -p 8080:8080 <image-name> -v `gravitino-trino-connector-<version>`:/usr/lib/trino/plugin/gravitino \
-v `gravitino-trino-cascading-connector-<version>`:/usr/lib/trino/plugin/trino

```

Similarly, to start Trino on the host `c2-trino` and mount the plugins, use:

```bash
docker run --name c2-trino -d -p 8080:8080 <image-name> -v `gravitino-trino-connector-<version>`:/usr/lib/trino/plugin/gravitino \
-v `gravitino-trino-cascading-connector-<version>`:/usr/lib/trino/plugin/trino
```

After starting the Trino containers, ensure the configuration directory `/etc/trino` is correctly set up.
Also, verify that the Trino containers on `c1-trino` and `c2-trino` can communicate with each other over the network.

## Configuring Trino

For detailed instructions on configuring Trino, please refer to the [Trino documentation](https://trino.io/docs/current/installation/deployment.html#configuring-trino).
After completing the basic configuration, proceed to configure the Gravitino connector.
Create a `gravitino.properties` file in the `etc/catalog` directory on the `c1-trino` host with the following contents:

```text
connector.name = gravitino
gravitino.uri = http://GRAVITINO_HOST_IP:GRAVITINO_HOST_PORT
gravitino.metalake = GRAVITINO_METALAKE_NAME
gravitino.cloud.region-code=c1
```

The property `gravitino.cloud.region-code=c1` specifies that the `c1-trino` host is in the `c1` region,
which will handle queries for catalogs in the `c1` region. For handling queries in the `c2` region,
they will be delegated to the `c2-trino` host.

Similarly, on the `c2-trino` host, create a `gravitino.properties` file in the `etc/catalog directory`:

```test
connector.name = gravitino
gravitino.uri = http://GRAVITINO_HOST_IP:GRAVITINO_HOST_PORT
gravitino.metalake = GRAVITINO_METALAKE_NAME
gravitino.cloud.region-code=c2
```

The `gravitino.cloud.region-code=c2` indicates that the `c2-trino` host is designated for the `c2` region,
thus queries for catalogs in this region will execute on c2-trino.

Ensure that the `gravitino.uri` setting on both `c1-trino` and `c2-trino` points to the same Gravitino server.
Verify that the server is operational and properly connected. Restart Trino after making any configuration changes.

## Creating Catalogs

To verify federated queries, create catalogs. 
Below is an example using the `gt_mysql` catalog in the `c2` region, configured for federated querying from `c1-trino`. 
Execute the following command in the `c1-trino` CLI to create the catalog:

To verify federated queries, start by creating catalogs. Below is an example of configuring the `gt_mysql` catalog
in the `c2` region for federated querying from `c1-trino`.
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

This configuration enables `c1-trino` to access `c2-trino` through the specified Trino JDBC connection information.
After successfully creating the catalog, use the Trino CLI on both `c1-trino` and `c2-trino` to view the catalogs.

```sql
SHOW CATALOGS;
```

## Adding Data

After creating the catalog, the next step is to add data to verify queries. Since the `Gravitino Trino cascading connector`
does not support data writing directly, execute the following commands using the `c2-trino` CLI to set up your data environment:


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

The output of the `c1-trino` CLI is the same as `c2-trino`.