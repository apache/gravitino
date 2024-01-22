CREATE SCHEMA "test.gt_mysql".gt_db1;

CREATE TABLE "test.gt_mysql".gt_db1.tb01 (
    name varchar(200),
    salary int,
    city int
);

alter table "test.gt_mysql".gt_db1.tb01 rename to "test.gt_mysql".gt_db1.tb03;
show tables from "test.gt_mysql".gt_db1;

alter table "test.gt_mysql".gt_db1.tb03 rename to "test.gt_mysql".gt_db1.tb01;
show tables from "test.gt_mysql".gt_db1;

alter table "test.gt_mysql".gt_db1.tb01 drop column city;
show create table "test.gt_mysql".gt_db1.tb01;

alter table "test.gt_mysql".gt_db1.tb01 alter column salary set data type bigint;
show create table "test.gt_mysql".gt_db1.tb01;

alter table "test.gt_mysql".gt_db1.tb01 add column city varchar(50) comment 'aaa';
show create table "test.gt_mysql".gt_db1.tb01;

drop table "test.gt_mysql".gt_db1.tb01;

drop schema "test.gt_mysql".gt_db1;
