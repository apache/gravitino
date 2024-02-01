CREATE SCHEMA "test.gt_mysql".gt_db1;

CREATE TABLE "test.gt_mysql".gt_db1.tb01 (
    name varchar(200),
    salary int
);

show create table "test.gt_mysql".gt_db1.tb01;

drop table "test.gt_mysql".gt_db1.tb01;

drop schema "test.gt_mysql".gt_db1;