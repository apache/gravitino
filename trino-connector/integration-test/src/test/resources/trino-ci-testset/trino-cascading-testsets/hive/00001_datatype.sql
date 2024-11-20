USE gt_hive1.gt_datatype;

SELECT * FROM tb02;

SELECT * FROM tb03;

SELECT * FROM tb04;

SELECT * FROM test_decimal_bounds;

SELECT SUM(value) FROM test_decimal_aggregation;

SELECT AVG(value) FROM test_decimal_aggregation;

SELECT val1 + val2 FROM test_decimal_arithmetic;

SELECT val1 * val2 FROM test_decimal_arithmetic;

SELECT val1 / val2 FROM test_decimal_arithmetic;

SELECT * FROM test_decimal_max_min ORDER BY max_min_val;

SELECT * FROM test_decimal_nulls;

SHOW CREATE SCHEMA gt_hive1.gt_datatype;

SHOW SCHEMAS LIKE 'gt_data%';

SHOW TABLES LIKE '%decimal_bounds';

SHOW COLUMNS FROM gt_hive1.gt_datatype.tb04;


