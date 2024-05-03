CREATE SCHEMA gt_mysql.gt_db1;

SHOW SCHEMAS FROM gt_mysql like 'gt_db1';

SHOW CREATE SCHEMA  gt_mysql.gt_db1;

CREATE SCHEMA gt_mysql.gt_db1;

CREATE SCHEMA IF NOT EXISTS gt_mysql.gt_db1;

CREATE SCHEMA IF NOT EXISTS gt_mysql.gt_db2;

SHOW SCHEMAS FROM gt_mysql like 'gt_db2';

CREATE TABLE gt_mysql.gt_db1.tb01 (
    name varchar(200),
    salary int
);

SHOW CREATE TABLE gt_mysql.gt_db1.tb01;

SHOW tables FROM gt_mysql.gt_db1 like 'tb01';

CREATE TABLE gt_mysql.gt_db1.tb01 (
     name varchar(200),
     salary int
);

CREATE TABLE IF NOT EXISTS gt_mysql.gt_db1.tb01 (
    name varchar(200),
    salary int
);

CREATE TABLE IF NOT EXISTS gt_mysql.gt_db1.tb02 (
    name varchar(200),
    salary int
);

SHOW tables FROM gt_mysql.gt_db1 like 'tb02';

DROP TABLE gt_mysql.gt_db1.tb01;

SHOW tables FROM gt_mysql.gt_db1 like 'tb01';

DROP TABLE gt_mysql.gt_db1.tb01;

DROP TABLE IF EXISTS gt_mysql.gt_db1.tb01;

DROP TABLE IF EXISTS gt_mysql.gt_db1.tb02;

SHOW tables FROM gt_mysql.gt_db1 like 'tb02';

DROP SCHEMA gt_mysql.gt_db1;

SHOW SCHEMAS FROM gt_mysql like 'gt_db1';

DROP SCHEMA IF EXISTS gt_mysql.gt_db1;

DROP SCHEMA IF EXISTS gt_mysql.gt_db2;

SHOW SCHEMAS FROM gt_mysql like 'gt_db2'
