CREATE SCHEMA "test.gt_mysql".gt_db1;

SHOW SCHEMAS FROM "test.gt_mysql" like 'gt_db1';

SHOW CREATE SCHEMA  "test.gt_mysql".gt_db1;

CREATE SCHEMA "test.gt_mysql".gt_db1;

CREATE SCHEMA IF NOT EXISTS "test.gt_mysql".gt_db1;

CREATE SCHEMA IF NOT EXISTS "test.gt_mysql".gt_db2;

SHOW SCHEMAS FROM "test.gt_mysql" like 'gt_db2';

CREATE TABLE "test.gt_mysql".gt_db1.tb01 (
    name varchar(200),
    salary int
);

SHOW CREATE TABLE "test.gt_mysql".gt_db1.tb01;

SHOW tables FROM "test.gt_mysql".gt_db1 like 'tb01';

CREATE TABLE "test.gt_mysql".gt_db1.tb01 (
     name varchar(200),
     salary int
);

CREATE TABLE IF NOT EXISTS "test.gt_mysql".gt_db1.tb01 (
    name varchar(200),
    salary int
);

CREATE TABLE IF NOT EXISTS "test.gt_mysql".gt_db1.tb02 (
    name varchar(200),
    salary int
);

SHOW tables FROM "test.gt_mysql".gt_db1 like 'tb02';

DROP TABLE "test.gt_mysql".gt_db1.tb01;

SHOW tables FROM "test.gt_mysql".gt_db1 like 'tb01';

DROP TABLE "test.gt_mysql".gt_db1.tb01;

DROP TABLE IF EXISTS "test.gt_mysql".gt_db1.tb01;

DROP TABLE IF EXISTS "test.gt_mysql".gt_db1.tb02;

SHOW tables FROM "test.gt_mysql".gt_db1 like 'tb02';

DROP SCHEMA "test.gt_mysql".gt_db1;

SHOW SCHEMAS FROM "test.gt_mysql" like 'gt_db1';

DROP SCHEMA IF EXISTS "test.gt_mysql".gt_db1;

DROP SCHEMA IF EXISTS "test.gt_mysql".gt_db2;

SHOW SCHEMAS FROM "test.gt_mysql" like 'gt_db2'
