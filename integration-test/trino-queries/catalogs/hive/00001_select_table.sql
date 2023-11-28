CREATE SCHEMA "test.hive".db1;

CREATE TABLE "test.hive".db1.tb01 (
    name varchar,
    salary int
)
WITH (
  format = 'TEXTFILE'
);

insert into "test.hive".db1.tb01(name, salary) values ('sam', 11);
insert into "test.hive".db1.tb01(name, salary) values ('jerry', 13);
insert into "test.hive".db1.tb01(name, salary) values ('bob', 14), ('tom', 12);

select * from "test.hive".db1.tb01 order by name;

drop table "test.hive".db1.tb01;

drop schema "test.hive".db1;
