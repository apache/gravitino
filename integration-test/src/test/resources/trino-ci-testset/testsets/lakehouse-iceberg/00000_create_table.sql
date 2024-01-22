CREATE SCHEMA "test.gt_iceberg".gt_db2;

CREATE TABLE "test.gt_iceberg".gt_db2.tb01 (
    name varchar,
    salary int
) with (
    partitioning = ARRAY['name'],
    sorted_by = ARRAY['salary']
);

show create table "test.gt_iceberg".gt_db2.tb01;

drop table "test.gt_iceberg".gt_db2.tb01;

drop schema "test.gt_iceberg".gt_db2;