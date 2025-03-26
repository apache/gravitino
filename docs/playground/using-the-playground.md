---
title: "Using the playground"
slug: /using-the-playground
keyword: playground
license: "This software is licensed under the Apache License version 2."
---

## Managing the playground

Once you have [installed](./install.md) the Apache Gravitino playground,
you can use the `playground.sh` script to manage its lifecycle.

- Starting the playground

  ```shell
  ./playground.sh start
  ```

- Check the status of the playground:

   ```shell 
   ./playground.sh status
   ```

- Stop the playground:

  ```shell
  ./playground.sh stop
  ```

## Using the individual components

### Experiment Apache Gravitino with Trino SQL

In the playground environment, you can try the Trino CLI in Docker container.

1. Exec into the Trino Docker container:

   ```shell
   docker exec -it playground-trino bash
   ```

1. Launch the Trino CLI in the container:

   ```shell
   trino@container_id:/$ trino
   ```

### Use Jupyter Notebook

1. Open the Jupyter Notebook in the browser at [http://localhost:18888](http://localhost:18888).

1. Open the `gravitino-trino-example.ipynb` notebook.

1. Start the notebook and run the cells.

### Using Spark client

1. Exec into the Spark Docker container:

   ```shell
   docker exec -it playground-spark bash
   ```

1. Open the Spark SQL client in the container.

   ```shell
   spark@container_id:/$ cd /opt/spark && /bin/bash bin/spark-sql
   ```

### Monitoring the playground

1. Open the Grafana in the browser at [http://localhost:13000](http://localhost:13000).

1. In the navigation menu, click *Dashboards* -> *Gravitino Playground*.

1. Experiment with the default template.

## Examples

### Simple Trino queries

You can use simple queries for testing's purpose in the Trino CLI.

```sql
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

In a typical organization, there are many departments using different data stacks.
In this example, the HR department uses Apache Hive to store its data;
the sales department uses PostgreSQL as its store.
With Gravitino, you can run some queries by joining data from the two departments.
For example, to find out which employee has the largest sales amount, run this SQL:

```sql
SELECT given_name, family_name, job_title, sum(total_amount) AS total_sales
FROM catalog_hive.sales.sales as s,
  catalog_postgres.hr.employees AS e
where s.employee_id = e.employee_id
GROUP BY given_name, family_name, job_title
ORDER BY total_sales DESC
LIMIT 1;
```

To identify the top customers who has the largest order amount by state, run this SQL:

```sql
SELECT customer_name, location, SUM(total_amount) AS total_spent
FROM catalog_hive.sales.sales AS s,
  catalog_hive.sales.stores AS l,
  catalog_hive.sales.customers AS c
WHERE s.store_id = l.store_id AND s.customer_id = c.customer_id
GROUP BY location, customer_name
ORDER BY location, SUM(total_amount) DESC;
```

To get the employees' average performance rating and total sales, run this SQL:

```sql
SELECT e.employee_id, given_name, family_name, AVG(rating) AS average_rating, SUM(total_amount) AS total_sales
FROM catalog_postgres.hr.employees AS e,
  catalog_postgres.hr.employee_performance AS p,
  catalog_hive.sales.sales AS s
WHERE e.employee_id = p.employee_id AND p.employee_id = s.employee_id
GROUP BY e.employee_id,  given_name, family_name;
```

### Using Spark and Trino

Consider generating data with SparkSQL and then querying this data using Trino.
Give it a try with Gravitino:

1. Login Spark container and execute the SQLs:

   ```sql
   // Create a table using the Hive catalog
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
   
   INSERT OVERWRITE TABLE employees PARTITION(department='Engineering')
     VALUES (1, 'John Doe', 30), (2, 'Jane Smith', 28);
   INSERT OVERWRITE TABLE employees PARTITION(department='Marketing')
     VALUES (3, 'Mike Brown', 32);
   ```

1. Exec into the Trino container and execute SQLs:

   ```sql
   SELECT * FROM catalog_hive.product.employees WHERE department = 'Engineering';
   ```

This demo is located in the `jupyter` folder in the playground project.
You can open the `gravitino-spark-trino-example.ipynb` in Jupyter Notebook
at [http://localhost:18888](http://localhost:18888).

### Using Apache Iceberg REST service

Suppose you want to migrate your business data from Hive to Iceberg.
Some data will be stored in Hive while other data will be using Iceberg.
Gravitino provides an Iceberg REST catalog service.
You can use Spark to access the REST catalog for writing data into the tables.
You can then use Trino to read the data from the Hive table and join them
with data from the Iceberg table.

The Spark configuration (`spark-defaults.conf`) is shown below.
It's already configured in the playground.

```text
spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.catalog_rest org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.catalog_rest.type rest
spark.sql.catalog.catalog_rest.uri http://gravitino:9001/iceberg/
spark.locality.wait.node 0
```

Please note that the `catalog_rest` in SparkSQL, the `catalog_iceberg` in Gravitino,
and Trino share the same Iceberg JDBC backend.
This means that they can access the same dataset.

1. Prepare some data for experiments

   Exec into the Spark container:

   ```shell
   docker exec -it playground-spark bash
   ```

   Run the following command inside the container:

   ```shell
   spark@container_id:/$ cd /opt/spark && /bin/bash bin/spark-sql
   ```

   Execute the following SQL statements in the SparkSQL console:

   ```SQL
   use catalog_rest;
   create database sales;
   use sales;
   create table customers (customer_id int, customer_name varchar(100), customer_email varchar(100));
   describe extended customers;
   insert into customers (customer_id, customer_name, customer_email) values (11,'Rory Brown','rory@123.com');
   insert into customers (customer_id, customer_name, customer_email) values (12,'Jerry Washington','jerry@dt.com');
   ```

1. Retrieve data using Trino

   You can get all the customers from both the Hive and Iceberg tables.
   First you will need to exec into the Trino container:

   ```shell
   docker exec -it playground-trino bash
   ```

   Launch the Trino CLI within the container:

   ```shell
   trino@container_id:/$ trino
   ```

   Run the following SQL in the Trino console:

   ```SQL
   select * from catalog_hive.sales.customers
   union
   select * from catalog_iceberg.sales.customers;
   ```

This demo is located in the `jupyter` folder in the playground project.
You can open the `gravitino-spark-trino-example.ipynb` demo in Jupyter Notebook
at [http://localhost:18888](http://localhost:18888).

### Using Gravitino with LlamaIndex

The Gravitino playground also provides a simple RAG demo with LlamaIndex.
This example shows you how to use Gravitino to manage both tabular and non-tabular datasets.
We'll connect to LlamaIndex and use it as a unified data source.
The example uses LlamaIndex and LLM to query both tabular and non-tabular data in one natural language query.

This demo is located in the `jupyter` folder in the playground project.
You can open the `gravitino_llama_index_demo.ipynb` demo in Jupyter Notebook
at [http://localhost:18888](http://localhost:18888).

In this example, the basic structured data about city statistics are  stored in MySQL.
The more detailed city introductions are stored in PDF files.
The user wants to find answers to some questions about some cities,
by checking both the structured data and the PDF files.

You will use Gravitino to manage the MySQL table using a relational catalog;
and you will store the PDF files using a fileset catalog.
Gravitino acts as a unified data source for LlamaIndex to build indice for
both the tabular and the non-tabular data.
You can then use LLM to query the data in natural language queries.

To run this demo, you need to set `OPENAI_API_KEY` in the `gravitino_llama_index_demo.ipynb`.
The `OPENAI_API_BASE` setting is optional.

```python
import os

os.environ["OPENAI_API_KEY"] = ""
os.environ["OPENAI_API_BASE"] = ""
```

<img src="https://analytics.apache.org/matomo.php?idsite=62&rec=1&bots=1&action_name=HowtoUsePlayground" alt="" />

