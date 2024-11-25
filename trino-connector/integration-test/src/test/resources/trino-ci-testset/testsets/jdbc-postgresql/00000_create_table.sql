CREATE SCHEMA gt_postgresql.gt_db1;

CREATE TABLE gt_postgresql.gt_db1.tb01 (
    name varchar,
    salary int
) COMMENT 'OKK';

SHOW CREATE TABLE gt_postgresql.gt_db1.tb01;

CREATE TABLE IF NOT EXISTS gt_postgresql.gt_db1.tb01 (
    name varchar,
    salary int
);

SHOW SCHEMAS FROM gt_postgresql like 'gt_db1';

SHOW CREATE SCHEMA gt_postgresql.gt_db1;

CREATE SCHEMA IF NOT EXISTS gt_postgresql.gt_db1;

SHOW TABLES FROM gt_postgresql.gt_db1 like 'tb01';

DROP TABLE IF EXISTS gt_postgresql.gt_db1.tb01;

DROP SCHEMA gt_postgresql.gt_db1;

DROP SCHEMA IF EXISTS gt_postgresql.gt_db1;

