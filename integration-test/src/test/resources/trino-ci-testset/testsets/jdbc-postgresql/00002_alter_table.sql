CREATE SCHEMA gt_postgresql.gt_db1;

CREATE TABLE gt_postgresql.gt_db1.tb01 (
    name varchar,
    salary int,
    city int
);

alter table gt_postgresql.gt_db1.tb01 rename to gt_postgresql.gt_db1.tb03;
show tables from gt_postgresql.gt_db1;

alter table gt_postgresql.gt_db1.tb03 rename to gt_postgresql.gt_db1.tb01;
show tables from gt_postgresql.gt_db1;

alter table gt_postgresql.gt_db1.tb01 drop column city;
show create table gt_postgresql.gt_db1.tb01;

alter table gt_postgresql.gt_db1.tb01 alter column salary set data type bigint;
show create table gt_postgresql.gt_db1.tb01;

comment on table gt_postgresql.gt_db1.tb01 is 'test table comments';
show create table gt_postgresql.gt_db1.tb01;

alter table gt_postgresql.gt_db1.tb01 rename column name to s;
show create table gt_postgresql.gt_db1.tb01;

comment on column gt_postgresql.gt_db1.tb01.s is 'test column comments';
show create table gt_postgresql.gt_db1.tb01;

alter table gt_postgresql.gt_db1.tb01 add column city varchar comment 'aaa';
show create table gt_postgresql.gt_db1.tb01;

drop table gt_postgresql.gt_db1.tb01;

drop schema gt_postgresql.gt_db1;
