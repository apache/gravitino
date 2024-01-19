CREATE SCHEMA "test.gt_postgresql".gt_db1;

CREATE TABLE "test.gt_postgresql".gt_db1.tb01 (
    name varchar,
    salary int,
    city int
);

alter table "test.gt_postgresql".gt_db1.tb01 rename to "test.gt_postgresql".gt_db1.tb03;
show tables from "test.gt_postgresql".gt_db1;

alter table "test.gt_postgresql".gt_db1.tb03 rename to "test.gt_postgresql".gt_db1.tb01;
show tables from "test.gt_postgresql".gt_db1;

alter table "test.gt_postgresql".gt_db1.tb01 drop column city;
show create table "test.gt_postgresql".gt_db1.tb01;

alter table "test.gt_postgresql".gt_db1.tb01 alter column salary set data type bigint;
show create table "test.gt_postgresql".gt_db1.tb01;

comment on table "test.gt_postgresql".gt_db1.tb01 is 'test table comments';
show create table "test.gt_postgresql".gt_db1.tb01;

alter table "test.gt_postgresql".gt_db1.tb01 rename column name to s;
show create table "test.gt_postgresql".gt_db1.tb01;

comment on column "test.gt_postgresql".gt_db1.tb01.s is 'test column comments';
show create table "test.gt_postgresql".gt_db1.tb01;

alter table "test.gt_postgresql".gt_db1.tb01 add column city varchar comment 'aaa';
show create table "test.gt_postgresql".gt_db1.tb01;

drop table "test.gt_postgresql".gt_db1.tb01;

drop schema "test.gt_postgresql".gt_db1;
