CREATE SCHEMA "test.gt_postgresql".gt_db1;

CREATE TABLE "test.gt_postgresql".gt_db1.tb01 (
    name varchar,
    salary int
);

show create table "test.gt_postgresql".gt_db1.tb01;

CREATE TABLE "test.gt_postgresql".gt_db1.tb02 (
    name varchar(100),
    salary int
);

show create table "test.gt_postgresql".gt_db1.tb02;

CREATE TABLE "test.gt_postgresql".gt_db1.tb03 (
    name char(100),
    salary int
);

show create table "test.gt_postgresql".gt_db1.tb03;

CREATE TABLE "test.gt_postgresql".gt_db1.tb04 (
    name char,
    salary int
);

show create table "test.gt_postgresql".gt_db1.tb04;

drop table "test.gt_postgresql".gt_db1.tb01;

drop table "test.gt_postgresql".gt_db1.tb02;

drop table "test.gt_postgresql".gt_db1.tb03;

drop table "test.gt_postgresql".gt_db1.tb04;

drop schema "test.gt_postgresql".gt_db1;