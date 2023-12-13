CREATE SCHEMA "test.lakehouse-iceberg".gt_db2;

CREATE TABLE "test.lakehouse-iceberg".gt_db2.tb01 (
    name varchar,
    salary int
);


insert into "test.lakehouse-iceberg".gt_db2.tb01(name, salary) values ('sam', 11);
insert into "test.lakehouse-iceberg".gt_db2.tb01(name, salary) values ('jerry', 13);
insert into "test.lakehouse-iceberg".gt_db2.tb01(name, salary) values ('bob', 14), ('tom', 12);

select * from "test.lakehouse-iceberg".gt_db2.tb01 order by name;

CREATE TABLE "test.lakehouse-iceberg".gt_db2.tb02 (
    name varchar,
    salary int
);

insert into "test.lakehouse-iceberg".gt_db2.tb02(name, salary) select distinct * from "test.lakehouse-iceberg".gt_db2.tb01 order by name;

select * from "test.lakehouse-iceberg".gt_db2.tb02 order by name;

drop table "test.lakehouse-iceberg".gt_db2.tb02;

drop table "test.lakehouse-iceberg".gt_db2.tb01;

drop schema "test.lakehouse-iceberg".gt_db2;
