CREATE SCHEMA gt_mysql.gt_db1;

CREATE TABLE gt_mysql.gt_db1.tb01 (
    name varchar,
    id int with ("auto_increment" = true)
);

drop table gt_mysql.gt_db1.tb01;

drop schema gt_mysql.gt_db1;