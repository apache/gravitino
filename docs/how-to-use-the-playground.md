---
title: "How to use the playground"
slug: /how-to-use-the-playground
keyword: playground
license: "This software is licensed under the Apache License version 2."
---

## Playground introduction

The playground is a complete Apache Gravitino Docker runtime environment with `Apache Hive`, `HDFS`, `Trino`, `MySQL`, `PostgreSQL`, `Jupyter`, and a `Apache Gravitino` server.

Depending on your network and computer, startup time may take 3-5 minutes. Once the playground environment has started, you can open [http://localhost:8090](http://localhost:8090) in a browser to access the Gravitino Web UI.

## Prerequisites

Install Git (optional), Docker, Docker Compose.

## System Resource Requirements

2 CPU cores, 8 GB RAM, 25 GB disk storage, MacOS or Linux OS (Verified Ubuntu22.04 Ubuntu24.04 AmazonLinux).

## TCP ports used

The playground runs several services. The TCP ports used may clash with existing services you run, such as MySQL or Postgres.

| Docker container      | Ports used             |
|-----------------------|------------------------|
| playground-gravitino  | 8090 9001              |
| playground-hive       | 3307 19000 19083 60070 |
| playground-mysql      | 13306                  |
| playground-postgresql | 15342                  |
| playground-trino      | 18080                  |
| playground-jupyter    | 18888                  |

## Playground usage

### Launch playground

You can launch the playground in two ways:

1. Use a single curl command

```shell
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/apache/gravitino-playground/HEAD/install.sh)"
```

2. Use Git

```shell
git clone git@github.com:apache/gravitino-playground.git
cd gravitino-playground
./playground.sh start
```

### Check status
```shell 
./playground.sh status
```
### Stop playground
```shell
./playground.sh stop
```

## Using Apache Gravitino with Trino SQL

### Using Trino CLI in Docker Container

1. Login to the Gravitino playground Trino Docker container using the following command:

```shell
docker exec -it playground-trino bash
````

2. Open the Trino CLI in the container.

```shell
trino@container_id:/$ trino
```

## Using Jupyter Notebook

1. Open the Jupyter Notebook in the browser at [http://localhost:18888](http://localhost:18888).

2. Open the `gravitino-trino-example.ipynb` notebook.

3. Start the notebook and run the cells.

## Using Spark client

1. Login to the Gravitino playground Spark Docker container using the following command:

```shell
docker exec -it playground-spark bash
````

2. Open the Spark SQL client in the container.

```shell
spark@container_id:/$ cd /opt/spark && /bin/bash bin/spark-sql 
```

## Example

### Simple Trino queries

You can use simple queries to test in the Trino CLI.

```SQL
SHOW CATALOGS;

CREATE SCHEMA catalog_hive.company
  WITH (location = 'hdfs://hive:9000/user/hive/warehouse/company.db');

SHOW CREATE SCHEMA catalog_hive.company;

CREATE TABLE catalog_hive.company.employees
(
  name varchar,
  salary decimal(10,2)
)
WITH (
  format = 'TEXTFILE'
);

INSERT INTO catalog_hive.company.employees (name, salary) VALUES ('Sam Evans', 55000);

SELECT * FROM catalog_hive.company.employees;

SHOW SCHEMAS from catalog_hive;

DESCRIBE catalog_hive.company.employees;

SHOW TABLES from catalog_hive.company;
```

### Cross-catalog queries

In a company, there may be different departments using different data stacks. In this example, the HR department uses Apache Hive to store its data, and the sales department uses PostgreSQL. You can run some interesting queries by joining the two departments' data together with Gravitino.

To know which employee has the largest sales amount, run this SQL:

```SQL
SELECT given_name, family_name, job_title, sum(total_amount) AS total_sales
FROM catalog_hive.sales.sales as s,
  catalog_postgres.hr.employees AS e
where s.employee_id = e.employee_id
GROUP BY given_name, family_name, job_title
ORDER BY total_sales DESC
LIMIT 1;
```

To know the top customers who bought the most by state, run this SQL:

```SQL
SELECT customer_name, location, SUM(total_amount) AS total_spent
FROM catalog_hive.sales.sales AS s,
  catalog_hive.sales.stores AS l,
  catalog_hive.sales.customers AS c
WHERE s.store_id = l.store_id AND s.customer_id = c.customer_id
GROUP BY location, customer_name
ORDER BY location, SUM(total_amount) DESC;
```

To know the employee's average performance rating and total sales, run this SQL:

```SQL
SELECT e.employee_id, given_name, family_name, AVG(rating) AS average_rating, SUM(total_amount) AS total_sales
FROM catalog_postgres.hr.employees AS e,
  catalog_postgres.hr.employee_performance AS p,
  catalog_hive.sales.sales AS s
WHERE e.employee_id = p.employee_id AND p.employee_id = s.employee_id
GROUP BY e.employee_id,  given_name, family_name;
```

### Using Spark and Trino

You might consider generating data with SparkSQL and then querying this data using Trino. Give it a try with Gravitino:

1. Login Spark container and execute the SQLs:

```sql
// using Hive catalog to create Hive table
USE catalog_hive;
CREATE DATABASE product;
USE product;

CREATE TABLE IF NOT EXISTS employees (
    id INT,
    name STRING,
    age INT
)
PARTITIONED BY (department STRING)
STORED AS PARQUET;
DESC TABLE EXTENDED employees;

INSERT OVERWRITE TABLE employees PARTITION(department='Engineering') VALUES (1, 'John Doe', 30), (2, 'Jane Smith', 28);
INSERT OVERWRITE TABLE employees PARTITION(department='Marketing') VALUES (3, 'Mike Brown', 32);
```

2. Login Trino container and execute SQLs:

```sql
SELECT * FROM catalog_hive.product.employees WHERE department = 'Engineering';
```


### Using Apache Iceberg REST service

Suppose you want to migrate your business from Hive to Iceberg. Some tables will use Hive, and the other tables will use Iceberg.
Gravitino provides an Iceberg REST catalog service. You can use Spark to access the REST catalog to write the table data.
Then, you can use Trino to read the data from the Hive table and join it with the Iceberg table.

`spark-defaults.conf` is as follows (It's already configured in the playground):

```text
spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.catalog_rest org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.catalog_rest.type rest
spark.sql.catalog.catalog_rest.uri http://gravitino:9001/iceberg/
spark.locality.wait.node 0
```

Please note that `catalog_rest` in SparkSQL and `catalog_iceberg` in Gravitino and Trino share the same Iceberg JDBC backend, which implies that they can access the same dataset.

1. Login Spark container and execute the steps.

```shell
docker exec -it playground-spark bash
```

```shell
spark@container_id:/$ cd /opt/spark && /bin/bash bin/spark-sql
```

```SQL
use catalog_rest;
create database sales;
use sales;
create table customers (customer_id int, customer_name varchar(100), customer_email varchar(100));
describe extended customers;
insert into customers (customer_id, customer_name, customer_email) values (11,'Rory Brown','rory@123.com');
insert into customers (customer_id, customer_name, customer_email) values (12,'Jerry Washington','jerry@dt.com');
```

2. Login Trino container and execute the steps.
You can get all the customers from both the Hive and Iceberg tables.

```shell
docker exec -it playground-trino bash
```

```shell
trino@container_id:/$ trino
```

```SQL
select * from catalog_hive.sales.customers
union
select * from catalog_iceberg.sales.customers;
```

### Using Gravitino with LlamaIndex

The Gravitino playground also provides a simple RAG demo with LlamaIndex. This demo will show you the
ability to use Gravitino to manage both tabular and non-tabular datasets, connecting to
LlamaIndex as a unified data source, then use LlamaIndex and LLM to query both tabular and
non-tabular data with one natural language query.

The demo is located in the `jupyter` folder, and you can open the `gravitino_llama_index_demo.ipynb`
demo via Jupyter Notebook by [http://localhost:18888](http://localhost:18888).

The scenario of this demo is that basic structured city statistics data is stored in MySQL, and
detailed city introductions are stored in PDF files. The user wants to find answers about cities in the structured data and the PDF files.

In this demo, you will use Gravitino to manage the MySQL table using a relational catalog, pdf
files using a fileset catalog, treating Gravitino as a unified data source for LlamaIndex to build
indexes on both tabular and non-tabular data. Then you will use LLM to query the data using natural
language queries.

Note: to run this demo, you need to set `OPENAI_API_KEY` in the `gravitino_llama_index_demo.ipynb`,
like below, `OPENAI_API_BASE` is optional.

```python
import os

os.environ["OPENAI_API_KEY"] = ""
os.environ["OPENAI_API_BASE"] = ""
```

<img src="https://analytics.apache.org/matomo.php?idsite=62&rec=1&bots=1&action_name=HowtoUsePlayground" alt="" />
