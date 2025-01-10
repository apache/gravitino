CREATE SCHEMA gt_hive.gt_decimal_db1;

USE gt_hive.gt_decimal_db1;

CREATE TABLE tb01 (amount DECIMAL(10, 2));

INSERT INTO tb01 VALUES (12345.67), (-9999999.99), (0.01);

INSERT INTO tb01 VALUES (123456789.00);  -- Exceeds precision

SELECT * FROM tb01;

CREATE TABLE tb02 (value DECIMAL(12, 3));

INSERT INTO tb02 VALUES (1234.567), (8901.234), (567.890);

SELECT SUM(value) FROM tb02;

SELECT AVG(value) FROM tb02;

CREATE TABLE tb03 (val1 DECIMAL(5, 2), val2 DECIMAL(4, 1));

INSERT INTO tb03 VALUES (123.45,10.1);

SELECT val1 + val2 FROM tb03;

SELECT val1 * val2 FROM tb03;

SELECT val1 / val2 FROM tb03;

CREATE TABLE tb04 (max_min_val DECIMAL(18, 4));

INSERT INTO tb04 VALUES (99999999999999.9999);

INSERT INTO tb04 VALUES (-99999999999999.9999);

INSERT INTO tb04 VALUES (100000000000000.0000); -- Exceeds max

SELECT * FROM tb04 ORDER BY max_min_val;

CREATE TABLE tb05 (nullable_val DECIMAL(8, 2));

INSERT INTO tb05 VALUES (NULL), (123.45), (NULL);

SELECT * FROM tb05;

DROP TABLE gt_hive.gt_decimal_db1.tb01;

DROP TABLE gt_hive.gt_decimal_db1.tb02;

DROP TABLE gt_hive.gt_decimal_db1.tb03;

DROP TABLE gt_hive.gt_decimal_db1.tb04;

DROP TABLE gt_hive.gt_decimal_db1.tb05;

DROP SCHEMA gt_hive.gt_decimal_db1;