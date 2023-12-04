CREATE SCHEMA "test.hive".db1;

CREATE TABLE "test.hive".db1.tb01 (
    name varchar,
    salary int
)
WITH (
  format = 'TEXTFILE'
);

drop table "test.hive".db1.tb01;

drop schema "test.hive".db1;