CREATE SCHEMA "test.lakehouse-iceberg".db1;

CREATE TABLE "test.lakehouse-iceberg".db1.tb01 (
    name varchar,
    salary int,
    city int
);

alter table "test.lakehouse-iceberg".db1.tb01 rename to "test.lakehouse-iceberg".db1.tb02;
show tables from "test.lakehouse-iceberg".db1;

alter table "test.lakehouse-iceberg".db1.tb02 rename to "test.lakehouse-iceberg".db1.tb01;
show tables from "test.lakehouse-iceberg".db1;

alter table "test.lakehouse-iceberg".db1.tb01 drop column city;
show create table "test.lakehouse-iceberg".db1.tb01;

alter table "test.lakehouse-iceberg".db1.tb01 rename column name to s;
show create table "test.lakehouse-iceberg".db1.tb01;

alter table "test.lakehouse-iceberg".db1.tb01 alter column salary set data type bigint;
show create table "test.lakehouse-iceberg".db1.tb01;

comment on table "test.lakehouse-iceberg".db1.tb01 is 'test table comments';
show create table "test.lakehouse-iceberg".db1.tb01;

comment on column "test.lakehouse-iceberg".db1.tb01.s is 'test column comments';
show create table "test.lakehouse-iceberg".db1.tb01;

drop table "test.lakehouse-iceberg".db1.tb01;

drop schema "test.lakehouse-iceberg".db1;
