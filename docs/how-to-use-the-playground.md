---
title: "How to use the playground"
slug: /how-to-use-the-playground
keyword: playground
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## Playground introduction

Playground is a complete Gravitino Docker runtime environment with `Hive`, `Hdfs`, `Trino`, `MySQL`, `PostgreSQL`, and `Gravitno` server.

Depending on your network, the startup may take 3-5 minutes.

Once the playground environment has started, you can open http://localhost:8090 to access the Gravitino Web UI.

## Prerequisite

You should install git and docker-compose.

## Start playground

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
In this example, HR department uses Apache Hive to store its data.
Sales department uses PostgreSQL to store its data.
This example has generated some data for two departments.
You can query some interesting results with Gravitino.

If you want to know which employee has the largest sales amount.
You can run the SQL.

```SQL
WITH totalsales AS (
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
  FROM totalsales
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

If you want to know top 10 customers who bought the most by state.
You run the SQL.

```SQL
WITH customersales AS (
    SELECT
        "metalake_demo.catalog_demo".sales.customers.customer_id,
        customer_name,
        customer_email,
        location AS state,
        SUM(total_amount) AS total_spent
    FROM "metalake_demo.catalog_demo".sales.sales
             JOIN "metalake_demo.catalog_demo".sales.customers
                  ON "metalake_demo.catalog_demo".sales.sales.customer_id = "metalake_demo.catalog_demo".sales.customers.customer_id
             JOIN "metalake_demo.catalog_demo".sales.stores
                  ON "metalake_demo.catalog_demo".sales.sales.store_id = "metalake_demo.catalog_demo".sales.stores.store_id
    GROUP BY
        "metalake_demo.catalog_demo".sales.customers.customer_id,
        customer_name,
        customer_email,
        location
), rankedcustomersales AS (
    SELECT
        customer_id,
        customer_name,
        customer_email,
        state,
        total_spent,
        RANK() OVER (PARTITION BY state ORDER BY total_spent DESC) AS customer_rank
    FROM customersales
)
SELECT
    customer_id,
    customer_name,
    customer_email,
    state,
    total_spent
FROM rankedcustomersales
WHERE
    customer_rank <= 10
ORDER BY
    state,
    customer_rank;
```

If you want to know that employees average performance rating and total sales.
You run the SQL.

```SQL
set session allow_pushdown_into_connectors=false;
WITH employeeperformance AS (
  SELECT
    employee_id,
    AVG(rating) AS average_rating
  FROM "metalake_demo.catalog_pg1".hr.employee_performance
  GROUP BY
    employee_id
), employeesales AS (
  SELECT
    employee_id,
    SUM(total_amount) AS total_sales
  FROM "metalake_demo.catalog_demo".sales.sales
  GROUP BY
    employee_id
)
SELECT
  e.employee_id,
  average_rating,
  total_sales
FROM employeeperformance AS e
JOIN employeesales AS s
  ON e.employee_id = s.employee_id;
```
