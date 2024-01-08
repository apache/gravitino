CREATE SCHEMA "test.jdbc-mysql".gt_db1;

USE "test.jdbc-mysql".gt_db1;

CREATE TABLE tb01 (
    name varchar,
    salary int
);

show create table tb01;

show tables from "test.jdbc-mysql".gt_db1;

drop table tb01;


drop schema "test.jdbc-mysql".gt_db1;
