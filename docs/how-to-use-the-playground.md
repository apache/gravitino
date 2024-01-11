---
title: "How to use the playground"
slug: /how-to-use-the-playground
keyword: playground
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## Playground introduction

The playground is a complete Gravitino Docker runtime environment with `Hive`, `HDFS`, `Trino`, `MySQL`, `PostgreSQL`, and a `Gravitino` server.

Depending on your network and computer, startup time may take 3-5 minutes. Once the playground environment has started, you can open <http://localhost:8090> in a browser to access the Gravitino Web UI.

## Prerequisites

You first need to install git and docker-compose.

## Start playground

```shell
git clone git@github.com:datastrato/gravitino-playground.git
cd gravitino-playground
./launch-playground.sh
```

## Experiencing Gravitino with Trino SQL

1. Login to the Gravitino playground Trino Docker container using the following command.

```shell
docker exec -it playground-trino bash
````

2. Open the Trino CLI in the container.

```shell
trino@d2bbfccc7432:/$ trino
```

## Example

### Simple queries

You can use simple queries to test in the Trino CLI.

```SQL
SHOW CATALOGS;

CREATE SCHEMA "metalake_demo.catalog_hive".company
  WITH (location = 'hdfs://hive:9000/user/hive/warehouse/company.db');

SHOW CREATE SCHEMA "metalake_demo.catalog_hive".company;

CREATE TABLE "metalake_demo.catalog_hive".company.employees
(
  name varchar,
  salary decimal(10,2)
)
WITH (
  format = 'TEXTFILE'
);

INSERT INTO "metalake_demo.catalog_hive".company.employees (name, salary) VALUES ('Sam Evans', 55000);

SELECT * FROM "metalake_demo.catalog_hive".company.employees;

SHOW SCHEMAS from "metalake_demo.catalog_hive";

DESCRIBE "metalake_demo.catalog_hive".company.employees;

SHOW TABLES from "metalake_demo.catalog_hive".company;
```

### Cross-catalog queries

In a company, there may be different departments using different data stacks. In this example, the HR department uses Apache Hive to store its data and the sales department uses PostgreSQL to store its data. You can run some interesting queries by joining the two departments' data together with Gravitino.

If you want to know which employee has the largest sales amount, you can run this SQL.

```SQL
SELECT given_name, family_name, job_title, sum(total_amount) AS total_sales
FROM "metalake_demo.catalog_hive".sales.sales as s,
  "metalake_demo.catalog_postgres".hr.employees AS e
where s.employee_id = e.employee_id
GROUP BY given_name, family_name, job_title
ORDER BY total_sales DESC
LIMIT 1;
```

If you want to know the top customers who bought the most by state, you can run this SQL.

```SQL
SELECT customer_name, location, SUM(total_amount) AS total_spent
FROM "metalake_demo.catalog_hive".sales.sales AS s,
  "metalake_demo.catalog_hive".sales.stores AS l,
  "metalake_demo.catalog_hive".sales.customers AS c
WHERE s.store_id = l.store_id AND s.customer_id = c.customer_id
GROUP BY location, customer_name
ORDER BY location, SUM(total_amount) DESC;
```

If you want to know the employee's average performance rating and total sales, you can run this SQL.

```SQL
SELECT e.employee_id, given_name, family_name, AVG(rating) AS average_rating,  SUM(total_amount) AS total_sales
FROM "metalake_demo.catalog_postgres".hr.employees AS e,
  "metalake_demo.catalog_postgres".hr.employee_performance AS p,
  "metalake_demo.catalog_hive".sales.sales AS s
WHERE e.employee_id = p.employee_id AND p.employee_id = s.employee_id
GROUP BY e.employee_id,  given_name, family_name;
```
