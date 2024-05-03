CREATE SCHEMA gt_hive.gt_db1;

CREATE TABLE gt_hive.gt_db1.tb01 (
    name varchar,
    salary int
)
WITH (
  format = 'TEXTFILE'
);

drop table gt_hive.gt_db1.tb01;

drop schema gt_hive.gt_db1;