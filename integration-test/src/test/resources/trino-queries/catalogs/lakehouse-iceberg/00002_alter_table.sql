CREATE SCHEMA "test.lakehouse-iceberg".gt_db2;

CREATE TABLE "test.lakehouse-iceberg".gt_db2.tb01 (
    name varchar,
    salary int,
    city int
);

alter table "test.lakehouse-iceberg".gt_db2.tb01 rename to "test.lakehouse-iceberg".gt_db2.tb03;
show tables from "test.lakehouse-iceberg".gt_db2;

alter table "test.lakehouse-iceberg".gt_db2.tb03 rename to "test.lakehouse-iceberg".gt_db2.tb01;
show tables from "test.lakehouse-iceberg".gt_db2;

alter table "test.lakehouse-iceberg".gt_db2.tb01 drop column city;
show create table "test.lakehouse-iceberg".gt_db2.tb01;

alter table "test.lakehouse-iceberg".gt_db2.tb01 rename column name to s;
show create table "test.lakehouse-iceberg".gt_db2.tb01;

alter table "test.lakehouse-iceberg".gt_db2.tb01 alter column salary set data type bigint;
show create table "test.lakehouse-iceberg".gt_db2.tb01;

comment on table "test.lakehouse-iceberg".gt_db2.tb01 is 'test table comments';
show create table "test.lakehouse-iceberg".gt_db2.tb01;

comment on column "test.lakehouse-iceberg".gt_db2.tb01.s is 'test column comments';
show create table "test.lakehouse-iceberg".gt_db2.tb01;

drop table "test.lakehouse-iceberg".gt_db2.tb01;

drop schema "test.lakehouse-iceberg".gt_db2;
