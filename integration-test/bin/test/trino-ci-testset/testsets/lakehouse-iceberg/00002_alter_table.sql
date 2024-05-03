CREATE SCHEMA gt_iceberg.gt_db2;

CREATE TABLE gt_iceberg.gt_db2.tb01 (
    name varchar,
    salary int,
    city int
);

alter table gt_iceberg.gt_db2.tb01 rename to gt_iceberg.gt_db2.tb03;
show tables from gt_iceberg.gt_db2;

alter table gt_iceberg.gt_db2.tb03 rename to gt_iceberg.gt_db2.tb01;
show tables from gt_iceberg.gt_db2;

alter table gt_iceberg.gt_db2.tb01 drop column city;
show create table gt_iceberg.gt_db2.tb01;

alter table gt_iceberg.gt_db2.tb01 rename column name to s;
show create table gt_iceberg.gt_db2.tb01;

alter table gt_iceberg.gt_db2.tb01 alter column salary set data type bigint;
show create table gt_iceberg.gt_db2.tb01;

comment on table gt_iceberg.gt_db2.tb01 is 'test table comments';
show create table gt_iceberg.gt_db2.tb01;

comment on column gt_iceberg.gt_db2.tb01.s is 'test column comments';
show create table gt_iceberg.gt_db2.tb01;

alter table gt_iceberg.gt_db2.tb01 add column city varchar comment 'aaa';
show create table gt_iceberg.gt_db2.tb01;

drop table gt_iceberg.gt_db2.tb01;

drop schema gt_iceberg.gt_db2;
