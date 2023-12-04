CREATE SCHEMA "test.hive".gt_db1;

CREATE TABLE "test.hive".gt_db1.tb01 (
    name varchar,
    salary int
)
WITH (
  format = 'TEXTFILE'
);

alter table "test.hive".gt_db1.tb01 add column city varchar comment 'aaa';

show create table "test.hive".gt_db1.tb01;