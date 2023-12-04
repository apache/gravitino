CREATE SCHEMA "test.lakehouse-iceberg".db2;

CREATE TABLE "test.lakehouse-iceberg".db2.tb01 (
    name varchar,
    salary int
);

drop table "test.lakehouse-iceberg".db2.tb01;

drop schema "test.lakehouse-iceberg".db2;