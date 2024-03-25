CREATE SCHEMA "test.gt_iceberg".varchar_db2;

USE "test.gt_iceberg".varchar_db2;

CREATE TABLE tb01 (id int, name char(20));

CREATE TABLE tb02 (id int, name char);

CREATE TABLE tb03 (id int, name varchar(233));

CREATE TABLE tb04 (id int, name varchar);

SHOW CREATE TABLE "test.gt_iceberg".varchar_db2.tb04;

drop table "test.gt_iceberg".varchar_db2.tb04;

drop schema "test.gt_iceberg".varchar_db2;

