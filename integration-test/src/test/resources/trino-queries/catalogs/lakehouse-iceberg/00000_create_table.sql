CREATE SCHEMA "test.lakehouse-iceberg".gt_db2;

CREATE TABLE "test.lakehouse-iceberg".gt_db2.tb01 (
    name varchar,
    salary int
);

drop table "test.lakehouse-iceberg".gt_db2.tb01;

drop schema "test.lakehouse-iceberg".gt_db2;