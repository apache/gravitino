CREATE SCHEMA "test.gt_iceberg".gt_db2;

CREATE TABLE "test.gt_iceberg".gt_db2.tb01 (
    name varchar,
    salary int
);

drop table "test.gt_iceberg".gt_db2.tb01;

drop schema "test.gt_iceberg".gt_db2;