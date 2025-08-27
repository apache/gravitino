CREATE DATABASE gt_hive_test_acid;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.support.concurrency=true;
CREATE TABLE gt_hive_test_acid.test_update
(
    id INT,
    name STRING,
    salary INT
)
CLUSTERED BY (id) INTO 4 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true');
CREATE TABLE gt_hive_test_acid.test_delete
(
    id INT,
    name STRING,
    salary INT
)
CLUSTERED BY (id) INTO 4 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true');
CREATE TABLE gt_hive_test_acid.test_merge
(
    id INT,
    name STRING,
    salary INT
)
CLUSTERED BY (id) INTO 4 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true');
