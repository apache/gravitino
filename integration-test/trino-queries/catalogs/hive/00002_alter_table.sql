CREATE SCHEMA "test.hive".db1;

CREATE TABLE "test.hive".db1.tb01 (
    name varchar,
    salary int,
    city int
)
WITH (
  format = 'TEXTFILE'
);

alter table "test.hive".db1.tb01 rename to "test.hive".db1.tb02;
show tables from "test.hive".db1;

alter table "test.hive".db1.tb02 rename to "test.hive".db1.tb01;
show tables from "test.hive".db1;

alter table "test.hive".db1.tb01 drop column city;
show create table "test.hive".db1.tb01;

alter table "test.hive".db1.tb01 rename column name to s;
show create table "test.hive".db1.tb01;

alter table "test.hive".db1.tb01 alter column s set data type varchar(256);
show create table "test.hive".db1.tb01;

comment on table "test.hive".db1.tb01 is 'test table comments';
show create table "test.hive".db1.tb01;

comment on column "test.hive".db1.tb01.s is 'test column comments';
show create table "test.hive".db1.tb01;

drop table "test.hive".db1.tb01;

drop schema "test.hive".db1;
