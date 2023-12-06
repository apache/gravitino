CREATE SCHEMA "test.hive".gt_db1;

CREATE TABLE "test.hive".gt_db1.tb01 (
    name varchar,
    salary int
)
WITH (
  format = 'TEXTFILE'
);

drop table "test.hive".gt_db1.tb01;

drop schema "test.hive".gt_db1;