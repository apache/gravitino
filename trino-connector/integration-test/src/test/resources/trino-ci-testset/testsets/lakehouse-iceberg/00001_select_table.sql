CREATE SCHEMA gt_db2;

CREATE TABLE gt_db2.tb01 (
    name varchar,
    salary int
);


insert into gt_db2.tb01(name, salary) values ('sam', 11);
insert into gt_db2.tb01(name, salary) values ('jerry', 13);
insert into gt_db2.tb01(name, salary) values ('bob', 14), ('tom', 12);

select * from gt_db2.tb01 order by name;

CREATE TABLE gt_db2.tb02 (
    name varchar,
    salary int
);

insert into gt_db2.tb02(name, salary) select * from gt_db2.tb01 order by name;

select * from gt_db2.tb02 order by name;

drop table gt_db2.tb02;

drop table gt_db2.tb01;

drop schema gt_db2;
