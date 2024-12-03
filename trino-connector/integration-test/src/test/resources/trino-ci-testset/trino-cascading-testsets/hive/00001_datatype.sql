CREATE TABLE gt_hive1_1.gt_datatype.tb02 (name char(255));

INSERT INTO gt_hive1_1.gt_datatype.tb02 (name) VALUES ('Apache Gravitino is a high-performance, geo-distributed, and federated metadata lake. It manages metadata directly in different sources, types, and regions, providing users with unified metadata access for data and AI assets.');

SELECT * FROM gt_hive1.gt_datatype.tb02;

SELECT * FROM gt_hive1.gt_datatype.tb03;

CREATE TABLE gt_hive1_1.gt_datatype.tb04 (name varchar);
INSERT INTO gt_hive1_1.gt_datatype.tb04 VALUES ('test abc');

SELECT * FROM gt_hive1.gt_datatype.tb04;

CREATE TABLE gt_hive1_1.gt_datatype.test_decimal_bounds (amount DECIMAL(10, 2));
INSERT INTO gt_hive1_1.gt_datatype.test_decimal_bounds VALUES (12345.67), (-9999999.99), (0.01);

SELECT * FROM gt_hive1.gt_datatype.test_decimal_bounds;


CREATE TABLE gt_hive1_1.gt_datatype.test_decimal_aggregation (value DECIMAL(12, 3));

INSERT INTO gt_hive1_1.gt_datatype.test_decimal_aggregation VALUES (1234.567), (8901.234), (567.890);

SELECT SUM(value) FROM gt_hive1.gt_datatype.test_decimal_aggregation;

SELECT AVG(value) FROM gt_hive1.gt_datatype.test_decimal_aggregation;


CREATE TABLE gt_hive1_1.gt_datatype.test_decimal_arithmetic (val1 DECIMAL(5, 2), val2 DECIMAL(4, 1));

INSERT INTO gt_hive1_1.gt_datatype.test_decimal_arithmetic VALUES (123.45,10.1);

SELECT val1 + val2 FROM gt_hive1.gt_datatype.test_decimal_arithmetic;

SELECT val1 * val2 FROM gt_hive1.gt_datatype.test_decimal_arithmetic;

SELECT val1 / val2 FROM gt_hive1.gt_datatype.test_decimal_arithmetic;


CREATE TABLE gt_hive1_1.gt_datatype.test_decimal_max_min (max_min_val DECIMAL(18, 4));

INSERT INTO gt_hive1_1.gt_datatype.test_decimal_max_min VALUES (99999999999999.9999);

INSERT INTO gt_hive1_1.gt_datatype.test_decimal_max_min VALUES (-99999999999999.9999);

SELECT * FROM gt_hive1.gt_datatype.test_decimal_max_min ORDER BY max_min_val;


CREATE TABLE gt_hive1_1.gt_datatype.test_decimal_nulls (nullable_val DECIMAL(8, 2));

INSERT INTO gt_hive1_1.gt_datatype.test_decimal_nulls VALUES (NULL), (123.45), (NULL);

SELECT * FROM gt_hive1.gt_datatype.test_decimal_nulls;

USE gt_hive1.gt_datatype;

SHOW CREATE SCHEMA gt_hive1.gt_datatype;

SHOW SCHEMAS LIKE 'gt_data%';

SHOW TABLES LIKE '%decimal_bounds';

SHOW COLUMNS FROM gt_hive1.gt_datatype.tb04;


