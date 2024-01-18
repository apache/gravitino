CREATE SCHEMA "test.gt_mysql".gt_db1;

CREATE TABLE "test.gt_mysql".gt_db1.tb01 (
    name varchar(200),
    salary int
);

insert into "test.gt_mysql".gt_db1.tb01(name, salary) values ('sam', 11);
insert into "test.gt_mysql".gt_db1.tb01(name, salary) values ('jerry', 13);
insert into "test.gt_mysql".gt_db1.tb01(name, salary) values ('bob', 14), ('tom', 12);

select * from "test.gt_mysql".gt_db1.tb01 order by name;

CREATE TABLE "test.gt_mysql".gt_db1.tb02 (
    name varchar(200),
    salary int
);

insert into "test.gt_mysql".gt_db1.tb02(name, salary) select distinct * from "test.gt_mysql".gt_db1.tb01 order by name;

select * from "test.gt_mysql".gt_db1.tb02 order by name;

select * from "test.gt_mysql".gt_db1.tb01 join "test.gt_mysql".gt_db1.tb02 t on tb01.salary = t.salary order by tb01.name;

drop table "test.gt_mysql".gt_db1.tb02;

drop table "test.gt_mysql".gt_db1.tb01;

drop schema "test.gt_mysql".gt_db1;
