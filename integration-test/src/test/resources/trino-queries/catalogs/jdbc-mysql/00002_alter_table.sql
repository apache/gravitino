CREATE SCHEMA "test.jdbc-mysql".gt_db1;

CREATE TABLE "test.jdbc-mysql".gt_db1.tb01 (
    name varchar,
    salary int,
    city int
);

alter table "test.jdbc-mysql".gt_db1.tb01 rename to "test.jdbc-mysql".gt_db1.tb03;
show tables from "test.jdbc-mysql".gt_db1;

alter table "test.jdbc-mysql".gt_db1.tb03 rename to "test.jdbc-mysql".gt_db1.tb01;
show tables from "test.jdbc-mysql".gt_db1;

alter table "test.jdbc-mysql".gt_db1.tb01 drop column city;
show create table "test.jdbc-mysql".gt_db1.tb01;

alter table "test.jdbc-mysql".gt_db1.tb01 alter column salary set data type bigint;
show create table "test.jdbc-mysql".gt_db1.tb01;

alter table "test.jdbc-mysql".gt_db1.tb01 add column city varchar comment 'aaa';
show create table "test.jdbc-mysql".gt_db1.tb01;

drop table "test.jdbc-mysql".gt_db1.tb01;

drop schema "test.jdbc-mysql".gt_db1;
