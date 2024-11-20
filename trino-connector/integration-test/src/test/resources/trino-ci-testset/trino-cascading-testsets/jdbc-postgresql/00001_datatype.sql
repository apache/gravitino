USE gt_postgresql1.gt_datatype;

select * from tb02 order by name;

USE gt_postgresql1.gt_varchar_db1;

SHOW CREATE TABLE test_char01;

SHOW CREATE TABLE test_char02;

SHOW CREATE TABLE test_char03;

SHOW CREATE TABLE test_varchar04;

SHOW CREATE TABLE test_varchar05;

SHOW CREATE TABLE test_varchar06;

SHOW CREATE SCHEMA gt_postgresql1.gt_push_db1;

SHOW SCHEMAS LIKE 'gt_push_%1';

USE gt_postgresql1.gt_push_db1;

SHOW TABLES LIKE 'cus%';

SHOW COLUMNS FROM gt_postgresql1.gt_push_db1.customer;