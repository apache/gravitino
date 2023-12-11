---
title: "How to use the playground"
slug: /how-to-use-the-playground
keyword: playground
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## Playground introduction

Playground is a complete Gravitino Docker runtime environment with `Hive`, `Hdfs`, `Trino`, `Mysql`, `Postgresql`, and `Gravitno` server.

Depending on your network, the startup may take 3-5 minutes.

Once the playground environment has started, you can open http://localhost:8090 to access the Gravitino Web UI.

## Prerequisite

You should install git and docker-compose.

## Startup playground

```shell
git clone git@github.com:datastrato/gravitino-playground.git
cd gravitino-playground
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

## Example

### Simple queries

Use simple queries to test in the Trino CLI.

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

### Cross-catalog queries

In companies, there may be different departments using different data stacks. 

In this sample, HR department uses Apache Hive to store its data.

Sales department uses Postgresql to store its data.

This sample have generated some data for two departments.

You can queries some interesting results use Gravitino. 

If you want to know which employee has the largest sales amount.

You can run the SQL.

```SQL
WITH salesscores AS (
  SELECT
    employee_id,
    SUM(total_amount) AS sales_amount
  FROM "metalake_demo.catalog_demo".sales.sales
  GROUP BY
    employee_id
), rankedemployees AS (
  SELECT
    employee_id,
    sales_amount,
    RANK() OVER (ORDER BY sales_amount DESC) AS sales_rank
  FROM salesscores
)
SELECT
  e.employee_id,
  given_name,
  family_name,
  job_title,
  sales_amount
FROM rankedemployees AS r
JOIN "metalake_demo.catalog_pg1".hr.employees AS e
  ON r.employee_id = e.employee_id
WHERE
  sales_rank = 1;
```


