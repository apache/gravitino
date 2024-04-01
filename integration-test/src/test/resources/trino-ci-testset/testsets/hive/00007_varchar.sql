CREATE SCHEMA "test.gt_hive".varchar_db1;

USE "test.gt_hive".varchar_db1;

CREATE TABLE tb01 (id int, name char(20));

SHOW CREATE TABLE "test.gt_hive".varchar_db1.tb01;

CREATE TABLE tb02 (id int, name char(255));

SHOW CREATE TABLE "test.gt_hive".varchar_db1.tb02;

CREATE TABLE tb03 (id int, name char(256));

CREATE TABLE tb04 (id int, name varchar(250));

SHOW CREATE TABLE "test.gt_hive".varchar_db1.tb04;

CREATE TABLE tb05 (id int, name varchar(65535));

SHOW CREATE TABLE "test.gt_hive".varchar_db1.tb05;

CREATE TABLE tb06 (id int, name char);

SHOW CREATE TABLE "test.gt_hive".varchar_db1.tb06;

CREATE TABLE tb07 (id int, name varchar);

SHOW CREATE TABLE "test.gt_hive".varchar_db1.tb07;

CREATE TABLE tb08 (id int, name varchar(65536));


drop table "test.gt_hive".varchar_db1.tb01;

drop table "test.gt_hive".varchar_db1.tb02;

drop table "test.gt_hive".varchar_db1.tb04;

drop table "test.gt_hive".varchar_db1.tb05;

drop table "test.gt_hive".varchar_db1.tb06;

drop table "test.gt_hive".varchar_db1.tb07;

drop schema "test.gt_hive".varchar_db1;

