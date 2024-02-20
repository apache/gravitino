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

comment on column "test.gt_mysql".gt_db1.tb01.name is 'test column comments';
show create table "test.gt_mysql".gt_db1.tb01;

comment on table "test.gt_mysql".gt_db1.tb01 is 'test table comments';
show create table "test.gt_mysql".gt_db1.tb01;

alter table "test.gt_mysql".gt_db1.tb01 rename column name to s;
show create table "test.gt_mysql".gt_db1.tb01;

-- alter table "test.gt_mysql".gt_db1.tb01 add column city varchar(50) not null comment 'aaa';
alter table "test.gt_mysql".gt_db1.tb01 add column city varchar(50) comment 'aaa';
show create table "test.gt_mysql".gt_db1.tb01;

alter table "test.gt_mysql".gt_db1.tb01 add column age int not null comment 'age of users';
show create table "test.gt_mysql".gt_db1.tb01;

alter table "test.gt_mysql".gt_db1.tb01 add column address varchar(200) not null comment 'address of users';
show create table "test.gt_mysql".gt_db1.tb01;

drop table "test.gt_mysql".gt_db1.tb01;

drop schema "test.gt_mysql".gt_db1;
