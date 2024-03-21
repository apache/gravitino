CREATE SCHEMA "test.gt_postgresql".gt_db1;

CREATE TABLE "test.gt_postgresql".gt_db1.tb01 (
    name varchar,
    salary int
);

insert into "test.gt_postgresql".gt_db1.tb01(name, salary) values ('sam', 11);
insert into "test.gt_postgresql".gt_db1.tb01(name, salary) values ('jerry', 13);
insert into "test.gt_postgresql".gt_db1.tb01(name, salary) values ('bob', 14), ('tom', 12);

select * from "test.gt_postgresql".gt_db1.tb01 order by name;

CREATE TABLE "test.gt_postgresql".gt_db1.tb02 (
    name varchar,
    salary int
);

insert into "test.gt_postgresql".gt_db1.tb02(name, salary) select distinct * from "test.gt_postgresql".gt_db1.tb01 order by name;

select * from "test.gt_postgresql".gt_db1.tb02 order by name;

drop table "test.gt_postgresql".gt_db1.tb02;

drop table "test.gt_postgresql".gt_db1.tb01;

drop schema "test.gt_postgresql".gt_db1;