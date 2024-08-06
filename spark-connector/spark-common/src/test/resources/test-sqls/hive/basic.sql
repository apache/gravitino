
-- If not specify location explicitly, the location will be hdfs://127.0.0.1:9000/xx, which is unaccessable for spark
CREATE DATABASE IF NOT EXISTS t_hive LOCATION '/user/hive/warehouse-spark-test/t_hive';
USE t_hive;

CREATE TABLE IF NOT EXISTS employees (
    id INT,
    name STRING,
    age INT
)
PARTITIONED BY (department STRING)
STORED AS PARQUET;

INSERT OVERWRITE TABLE employees PARTITION(department='Engineering') VALUES (1, 'John Doe', 30), (2, 'Jane Smith', 28);
INSERT OVERWRITE TABLE employees PARTITION(department='Marketing') VALUES (3, 'Mike Brown', 32);

SELECT * FROM employees WHERE department = 'Engineering';
