CREATE SCHEMA gt_iceberg_test_table_statistics;

CREATE TABLE gt_iceberg_test_table_statistics.tb01 (
    key1 int,
    value1 int
);

INSERT INTO gt_iceberg_test_table_statistics.tb01(key1, value1) VALUES (1, 1), (2, 2);

SHOW STATS FOR gt_iceberg_test_table_statistics.tb01;

DROP TABLE gt_iceberg_test_table_statistics.tb01;

DROP SCHEMA gt_iceberg_test_table_statistics;
