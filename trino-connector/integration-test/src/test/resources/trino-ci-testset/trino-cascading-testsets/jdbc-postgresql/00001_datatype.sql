INSERT INTO gt_postgresql1_1.gt_datatype.tb01(name, salary) VALUES ('sam', 11);
INSERT INTO gt_postgresql1_1.gt_datatype.tb01(name, salary) VALUES ('jerry', 13);
INSERT INTO gt_postgresql1_1.gt_datatype.tb01(name, salary) VALUES ('bob', 14), ('tom', 12);

CREATE TABLE gt_postgresql1_1.gt_datatype.tb02 (
    name varchar,
    salary int
);

INSERT INTO gt_postgresql1_1.gt_datatype.tb02(name, salary) SELECT * FROM gt_postgresql1_1.gt_datatype.tb01;

select * from gt_postgresql1.gt_datatype.tb02 order by name;

CREATE SCHEMA gt_postgresql1_1.gt_varchar_db1;

CREATE TABLE gt_postgresql1_1.gt_varchar_db1.test_char01 (id int, name char(20));

SHOW CREATE TABLE gt_postgresql1.gt_varchar_db1.test_char01;

CREATE TABLE gt_postgresql1_1.gt_varchar_db1.test_char02 (id int, name char(65536));

SHOW CREATE TABLE gt_postgresql1.gt_varchar_db1.test_char02;

CREATE TABLE gt_postgresql1_1.gt_varchar_db1.test_char03 (id int, name char);

SHOW CREATE TABLE gt_postgresql1.gt_varchar_db1.test_char03;

CREATE TABLE gt_postgresql1_1.gt_varchar_db1.test_varchar04 (id int, name varchar(250));

SHOW CREATE TABLE gt_postgresql1.gt_varchar_db1.test_varchar04;

CREATE TABLE gt_postgresql1_1.gt_varchar_db1.test_varchar05 (id int, name varchar(10485760));

SHOW CREATE TABLE gt_postgresql1.gt_varchar_db1.test_varchar05;

CREATE TABLE gt_postgresql1_1.gt_varchar_db1.test_varchar06 (id int, name varchar);

SHOW CREATE TABLE gt_postgresql1.gt_varchar_db1.test_varchar06;

CREATE SCHEMA gt_postgresql1_1.gt_push_db1;

SHOW CREATE SCHEMA gt_postgresql1.gt_push_db1;