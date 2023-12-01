CREATE SCHEMA "test.hive".db1;

CREATE TABLE "test.hive".db1.tb01 (
    name varchar,
    salary int
)
WITH (
  format = 'TEXTFILE'
);

alter table "test.hive".db1.tb01 add column city varchar comment 'aaa';

show create table "test.hive".db1.tb01;