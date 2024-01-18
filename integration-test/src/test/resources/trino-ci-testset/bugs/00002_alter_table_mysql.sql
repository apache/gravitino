CREATE SCHEMA "test.jdbc-mysql".gt_db1;

CREATE TABLE "test.jdbc-mysql".gt_db1.tb01 (
    name varchar,
    salary int
);

alter table "test.jdbc-mysql".gt_db1.tb01 rename column name to s;
show tables from "test.jdbc-mysql".gt_db1;

comment on table "test.jdbc-mysql".gt_db1.tb01 is 'test table comments';
show create table "test.jdbc-mysql".gt_db1.tb01;

comment on column "test.jdbc-mysql".gt_db1.tb01.s is 'test column comments';
show create table "test.jdbc-mysql".gt_db1.tb01;

