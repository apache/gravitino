CREATE SCHEMA gt_mysql.gt_db1;

CREATE TABLE gt_mysql.gt_db1.tb01 (
    name varchar(200),
    salary int
);

insert into gt_mysql.gt_db1.tb01(name, salary) values ('sam', 11);
insert into gt_mysql.gt_db1.tb01(name, salary) values ('jerry', 13);
insert into gt_mysql.gt_db1.tb01(name, salary) values ('bob', 14), ('tom', 12);

select * from gt_mysql.gt_db1.tb01 order by name;

CREATE TABLE gt_mysql.gt_db1.tb02 (
    name varchar(200),
    salary int
);

insert into gt_mysql.gt_db1.tb02(name, salary) select * from gt_mysql.gt_db1.tb01 order by name;

select * from gt_mysql.gt_db1.tb02 order by name;

select * from gt_mysql.gt_db1.tb01 join gt_mysql.gt_db1.tb02 t on tb01.salary = t.salary order by tb01.name;

drop table gt_mysql.gt_db1.tb02;

drop table gt_mysql.gt_db1.tb01;

drop schema gt_mysql.gt_db1;
