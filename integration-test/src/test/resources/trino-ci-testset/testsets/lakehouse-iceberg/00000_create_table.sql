CREATE SCHEMA "test.gt_iceberg".gt_db2;

CREATE TABLE "test.gt_iceberg".gt_db2.tb01(
    name   varchar,
    salary int
);

show create table "test.gt_iceberg".gt_db2.tb01;

CREATE TABLE "test.gt_iceberg".gt_db2.tb02 (
    name varchar,
    salary int
) with (
      partitioning = ARRAY['name'],
      sorted_by = ARRAY['salary']
    );

show create table "test.gt_iceberg".gt_db2.tb02;

CREATE TABLE "test.gt_iceberg".gt_db2.tb03 (
    name varchar,
    salary int
) with (
      partitioning = ARRAY['name'],
      sorted_by = ARRAY['salary_wrong_name']
    );

CREATE TABLE "test.gt_iceberg".gt_db2.tb03 (
    name varchar,
    salary int
) with (
      partitioning = ARRAY['name'],
      sorted_by = ARRAY['name']
      );

show create table "test.gt_iceberg".gt_db2.tb03;


CREATE TABLE "test.gt_iceberg".gt_db2.tb04 (
       name varchar,
       salary int
) with (
      sorted_by = ARRAY['name']
);

show create table "test.gt_iceberg".gt_db2.tb04;

CREATE TABLE "test.gt_iceberg".gt_db2.tb05 (
   name varchar,
   salary int
) with (
  partitioning = ARRAY['name']
);

show create table "test.gt_iceberg".gt_db2.tb05;

CREATE TABLE "test.gt_iceberg".gt_db2.tb06 (
   name varchar,
   salary int
) with (
  location = '${hdfs_uri}/user/iceberg/warehouse/TrinoQueryIT/gt_iceberg/gt_db2/tb06'
);

show create table "test.gt_iceberg".gt_db2.tb06;

drop table "test.gt_iceberg".gt_db2.tb01;

drop table "test.gt_iceberg".gt_db2.tb02;

drop table "test.gt_iceberg".gt_db2.tb03;

drop table "test.gt_iceberg".gt_db2.tb04;

drop table "test.gt_iceberg".gt_db2.tb05;

drop table "test.gt_iceberg".gt_db2.tb06;

drop schema "test.gt_iceberg".gt_db2;