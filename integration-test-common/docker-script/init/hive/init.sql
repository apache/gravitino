CREATE DATABASE gt_hive_test_acid;
CREATE TABLE gt_hive_test_acid.employees (id INT,name STRING,salary INT) CLUSTERED BY (id) INTO 4 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true');