<!--
  Copyright 2023 Datastrato.
  This software is licensed under the Apache License version 2.
-->
# Playground
This is a complete Gravitino Docker runtime environment with `Hive`, `Hdfs`, `Trino`, and `Gravitno` Server. just execute the `./launch-playground.sh` script.
Depending on your network, the startup may take 3-5 minutes.
Once the playground environment has started, you can open http://localhost:8090 to access the Gravitino Web UI.

## Startup playground
```shell
./launch-playground.sh
```

## Experience Gravitino with Trino SQL

1. Login to Gravitino playground Trino Docker container using the following command.

```shell
docker exec -it playground-trino bash
````

2. Open Trino CLI in the container.

```shell
trino@d2bbfccc7432:/$ trino
```

3. Use follow SQL to test in the Trino CLI.

```SQL
SHOW CATALOGS;

CREATE SCHEMA "metalake_demo.catalog_demo".db1
  WITH (location = 'hdfs://hive:9000/user/hive/warehouse/db1.db');

SHOW CREATE SCHEMA "metalake_demo.catalog_demo".db1;

CREATE TABLE "metalake_demo.catalog_demo".db1.table_001
(
  name varchar,
  salary varchar
)
WITH (
  format = 'TEXTFILE'
);

INSERT INTO "metalake_demo.catalog_demo".db1.table_001 (name, salary) VALUES ('sam', '11');

SELECT * FROM "metalake_demo.catalog_demo".db1.table_001;
```
