CREATE SCHEMA gt_hive.decimal_db1;

USE gt_hive.decimal_db1;

CREATE TABLE test_decimal_bounds (amount DECIMAL(10, 2));

INSERT INTO test_decimal_bounds VALUES (12345.67), (-9999999.99), (0.01);

INSERT INTO test_decimal_bounds VALUES (123456789.00);  -- Exceeds precision

SELECT * FROM test_decimal_bounds;

CREATE TABLE test_decimal_aggregation (value DECIMAL(12, 3));

INSERT INTO test_decimal_aggregation VALUES (1234.567), (8901.234), (567.890);

SELECT SUM(value) FROM test_decimal_aggregation;

SELECT AVG(value) FROM test_decimal_aggregation;

CREATE TABLE test_decimal_arithmetic (val1 DECIMAL(5, 2), val2 DECIMAL(4, 1));

INSERT INTO test_decimal_arithmetic VALUES (123.45,10.1);

SELECT val1 + val2 FROM test_decimal_arithmetic;

SELECT val1 * val2 FROM test_decimal_arithmetic;

SELECT val1 / val2 FROM test_decimal_arithmetic;

CREATE TABLE test_decimal_max_min (max_min_val DECIMAL(18, 4));

INSERT INTO test_decimal_max_min VALUES (99999999999999.9999);

INSERT INTO test_decimal_max_min VALUES (-99999999999999.9999);

INSERT INTO test_decimal_max_min VALUES (100000000000000.0000); -- Exceeds max

SELECT * FROM test_decimal_max_min ORDER BY max_min_val;

CREATE TABLE test_decimal_nulls (nullable_val DECIMAL(8, 2));

INSERT INTO test_decimal_nulls VALUES (NULL), (123.45), (NULL);

SELECT * FROM test_decimal_nulls;

DROP TABLE gt_hive.decimal_db1.test_decimal_bounds;

DROP TABLE gt_hive.decimal_db1.test_decimal_aggregation;

DROP TABLE gt_hive.decimal_db1.test_decimal_arithmetic;

DROP TABLE gt_hive.decimal_db1.test_decimal_max_min;

DROP TABLE gt_hive.decimal_db1.test_decimal_nulls;

DROP SCHEMA gt_hive.decimal_db1;