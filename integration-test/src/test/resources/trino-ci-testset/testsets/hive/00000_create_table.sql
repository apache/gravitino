CREATE SCHEMA "test.gt_hive".gt_db1;

CREATE TABLE "test.gt_hive".gt_db1.tb01 (
    name varchar,
    salary int
)
WITH (
  format = 'TEXTFILE'
);

drop table "test.gt_hive".gt_db1.tb01;

drop schema "test.gt_hive".gt_db1;