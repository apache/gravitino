CREATE SCHEMA "test.gt_mysql".gt_db1;

USE "test.gt_mysql".gt_db1;

CREATE TABLE tb01 (
    name varchar,
    salary int
);

show tables from "test.gt_mysql".gt_db1;

drop table tb01;

drop schema "test.gt_mysql".gt_db1;
