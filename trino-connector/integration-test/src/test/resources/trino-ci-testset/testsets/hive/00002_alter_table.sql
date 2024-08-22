CREATE SCHEMA gt_hive.gt_db1;

CREATE TABLE gt_hive.gt_db1.tb01 (
    name varchar,
    salary int,
    city int
)
WITH (
  format = 'TEXTFILE'
);

alter table gt_hive.gt_db1.tb01 rename to gt_hive.gt_db1.tb03;
show tables from gt_hive.gt_db1;

alter table gt_hive.gt_db1.tb03 rename to gt_hive.gt_db1.tb01;
show tables from gt_hive.gt_db1;

alter table gt_hive.gt_db1.tb01 drop column city;
show create table gt_hive.gt_db1.tb01;

alter table gt_hive.gt_db1.tb01 rename column name to s;
show create table gt_hive.gt_db1.tb01;

alter table gt_hive.gt_db1.tb01 alter column s set data type varchar(256);
show create table gt_hive.gt_db1.tb01;

comment on table gt_hive.gt_db1.tb01 is 'test table comments';
show create table gt_hive.gt_db1.tb01;

comment on column gt_hive.gt_db1.tb01.s is 'test column comments';
show create table gt_hive.gt_db1.tb01;

alter table gt_hive.gt_db1.tb01 add column city varchar comment 'aaa';
show create table gt_hive.gt_db1.tb01;

drop table gt_hive.gt_db1.tb01;

drop schema gt_hive.gt_db1;
