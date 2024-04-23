CREATE SCHEMA gt_postgresql.gt_db1;

CREATE TABLE gt_postgresql.gt_db1.tb01 (
    name varchar,
    id int not null with ("auto_increment" = true)
);

show create table gt_postgresql.gt_db1.tb01;

-- trino does not support insert into table with auto_increment column
-- insert into gt_postgresql.gt_db1.tb01 (name) values ('name01');  // failed: NULL value not allowed for NOT NULL column: id
-- insert into gt_postgresql.gt_db1.tb01 (name, id) values ('name01', 10); // success: but id is set by insert

drop table gt_postgresql.gt_db1.tb01;

drop schema gt_postgresql.gt_db1;