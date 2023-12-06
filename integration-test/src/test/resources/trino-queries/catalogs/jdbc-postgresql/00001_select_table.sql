CREATE SCHEMA "test.jdbc-postgresql".gt_db1;

CREATE TABLE "test.jdbc-postgresql".gt_db1.tb01 (
    name varchar,
    salary int
);

insert into "test.jdbc-postgresql".gt_db1.tb01(name, salary) values ('sam', 11);
insert into "test.jdbc-postgresql".gt_db1.tb01(name, salary) values ('jerry', 13);
insert into "test.jdbc-postgresql".gt_db1.tb01(name, salary) values ('bob', 14), ('tom', 12);

select * from "test.jdbc-postgresql".gt_db1.tb01 order by name;

CREATE TABLE "test.jdbc-postgresql".gt_db1.tb02 (
    name varchar,
    salary int
);

insert into "test.jdbc-postgresql".gt_db1.tb02(name, salary) select distinct * from "test.jdbc-postgresql".gt_db1.tb01 order by name;

select * from "test.jdbc-postgresql".gt_db1.tb02 order by name;

drop table "test.jdbc-postgresql".gt_db1.tb02;

drop table "test.jdbc-postgresql".gt_db1.tb01;

drop schema "test.jdbc-postgresql".gt_db1;