CREATE SCHEMA gt_postgresql.gt_db1;

CREATE TABLE gt_postgresql.gt_db1.tb01 (
    name varchar,
    salary int,
    city int
);

alter table gt_postgresql.gt_db1.tb01 rename to gt_postgresql.gt_db1.tb03;
show tables from gt_postgresql.gt_db1;

alter table gt_postgresql.gt_db1.tb03 rename to gt_postgresql.gt_db1.tb01;
show tables from gt_postgresql.gt_db1;

alter table gt_postgresql.gt_db1.tb01 drop column city;
show create table gt_postgresql.gt_db1.tb01;

alter table gt_postgresql.gt_db1.tb01 alter column salary set data type bigint;
show create table gt_postgresql.gt_db1.tb01;

comment on table gt_postgresql.gt_db1.tb01 is 'test table comments';
show create table gt_postgresql.gt_db1.tb01;

alter table gt_postgresql.gt_db1.tb01 rename column name to s;
show create table gt_postgresql.gt_db1.tb01;

comment on column gt_postgresql.gt_db1.tb01.s is 'test column comments';
show create table gt_postgresql.gt_db1.tb01;

alter table gt_postgresql.gt_db1.tb01 add column city varchar comment 'aaa';
show create table gt_postgresql.gt_db1.tb01;

SHOW COLUMNS FROM gt_postgresql.gt_db1.tb01;

SHOW COLUMNS FROM gt_postgresql.gt_db1.tb01 LIKE 's%';

ALTER TABLE IF EXISTS gt_postgresql.gt_db1.tb01 DROP COLUMN IF EXISTS created_at;

ALTER TABLE IF EXISTS gt_postgresql.gt_db1.tb01 RENAME COLUMN IF EXISTS available TO available_test;

CREATE TABLE gt_postgresql.gt_db1.tb02 (
    id INT,
    name VARCHAR(50)
);

INSERT INTO gt_postgresql.gt_db1.tb02 (id, name) VALUES (1, NULL);

-- ALTER TABLE gt_postgresql.gt_db1.tb02 ADD COLUMN gender boolean NOT NULL;

CREATE TABLE gt_postgresql.gt_db1.tb03 (
    id INT,
    name VARCHAR(50)
);

COMMENT ON COLUMN gt_postgresql.gt_db1.tb03.id is 'this is id';

ALTER TABLE gt_postgresql.gt_db1.tb03 ADD COLUMN gender boolean NOT NULL;

INSERT INTO gt_postgresql.gt_db1.tb03 (id, name, gender) VALUES (1, NULL, true);

SELECT * FROM gt_postgresql.gt_db1.tb03;

-- COMMENT ON COLUMN gt_postgresql.gt_db1.tb03.name is '';

SHOW CREATE TABLE gt_postgresql.gt_db1.tb03;

DROP TABLE gt_postgresql.gt_db1.tb03;

DROP TABLE gt_postgresql.gt_db1.tb02;

drop table gt_postgresql.gt_db1.tb01;

drop schema gt_postgresql.gt_db1;
