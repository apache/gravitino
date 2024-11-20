call gravitino.system.create_catalog(
    'gt_postgresql1',
    'jdbc-postgresql',
     MAP(
        ARRAY['jdbc-url', 'jdbc-user', 'jdbc-password', 'jdbc-database', 'jdbc-driver', 'trino.bypass.join-pushdown.strategy', 'cloud.region-code', 'cloud.trino.connection-url', 'cloud.trino.connection-user', 'cloud.trino.connection-password'],
        ARRAY['${postgresql_uri}/db', 'postgres', 'postgres', 'db', 'org.postgresql.Driver', 'EAGER','c2', '${trino_remote_jdbc_uri}', 'admin', '']
    )
);

call gravitino.system.create_catalog(
    'gt_postgresql1_1',
    'jdbc-postgresql',
    map(
        array['jdbc-url', 'jdbc-user', 'jdbc-password', 'jdbc-database', 'jdbc-driver', 'trino.bypass.join-pushdown.strategy'],
        array['${postgresql_uri}/db', 'postgres', 'postgres', 'db', 'org.postgresql.Driver', 'EAGER']
    )
);

CREATE SCHEMA gt_postgresql1_1.gt_datatype;
USE gt_postgresql1_1.gt_datatype;

CREATE TABLE tb01 (
    name varchar,
    salary int
);

INSERT INTO tb01(name, salary) VALUES ('sam', 11);
INSERT INTO tb01(name, salary) VALUES ('jerry', 13);
INSERT INTO tb01(name, salary) VALUES ('bob', 14), ('tom', 12);

CREATE TABLE tb02 (
    name varchar,
    salary int
);

INSERT INTO tb02(name, salary) SELECT * FROM tb01;

CREATE TABLE tb03 (
    f1 VARCHAR(200),
    f2 CHAR(20),
    f3 VARBINARY,
    f4 DECIMAL(10, 3),
    f5 REAL,
    f6 DOUBLE,
    f7 BOOLEAN,
    f9 SMALLINT,
    f10 INT,
    f11 INTEGER,
    f12 BIGINT,
    f13 DATE,
    f14 TIME,
    f15 TIMESTAMP,
    f16 TIMESTAMP WITH TIME ZONE
);

INSERT INTO tb03 (f1, f2, f3, f4, f5, f6, f7, f9, f10, f11, f12, f13, f14, f15, f16)
VALUES ('Sample text 1', 'Text1', x'65', 123.456, 7.89, 12.34, false, 100, 1000, 1000, 100000, DATE '2024-01-01',
        TIME '08:00:00', TIMESTAMP '2024-01-01 08:00:00', TIMESTAMP '2024-01-01 08:00:00 UTC');

INSERT INTO tb03 (f1, f2, f3, f4, f5, f6, f7, f9, f10, f11, f12, f13, f14, f15, f16)
VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

CREATE TABLE tb04 (
    f1 VARCHAR(200) not null ,
    f2 CHAR(20) not null ,
    f3 VARBINARY not null ,
    f4 DECIMAL(10, 3) not null ,
    f5 REAL not null ,
    f6 DOUBLE not null ,
    f7 BOOLEAN not null ,
    f9 SMALLINT not null ,
    f10 INT not null ,
    f11 INTEGER not null ,
    f12 BIGINT not null ,
    f13 DATE not null ,
    f14 TIME not null ,
    f15 TIMESTAMP not null,
    f16 TIMESTAMP WITH TIME ZONE not null
);

INSERT INTO tb04 (f1, f2, f3, f4, f5, f6, f7, f9, f10, f11, f12, f13, f14, f15, f16)
VALUES ('Sample text 1', 'Text1', x'65', 123.456, 7.89, 12.34, false, 100, 1000, 1000, 100000, DATE '2024-01-01',
        TIME '08:00:00', TIMESTAMP '2024-01-01 08:00:00', TIMESTAMP '2024-01-01 08:00:00 UTC');

CREATE SCHEMA gt_postgresql1_1.gt_varchar_db1;

USE gt_postgresql1_1.gt_varchar_db1;

CREATE TABLE test_char01 (id int, name char(20));

CREATE TABLE test_char02 (id int, name char(65536));

CREATE TABLE test_char03 (id int, name char);

CREATE TABLE test_varchar04 (id int, name varchar(250));

CREATE TABLE test_varchar05 (id int, name varchar(10485760));

CREATE TABLE test_varchar06 (id int, name varchar);

CREATE SCHEMA gt_postgresql1_1.gt_push_db1;

USE gt_postgresql1_1.gt_push_db1;

CREATE TABLE gt_postgresql1_1.gt_push_db1.employee_performance (
   employee_id integer,
   evaluation_date date,
   rating integer
)
COMMENT 'comment';

CREATE TABLE gt_postgresql1_1.gt_push_db1.employees (
  employee_id integer,
  department_id integer,
  job_title varchar(100),
  given_name varchar(100),
  family_name varchar(100),
  birth_date date,
  hire_date date
)
COMMENT 'comment';

INSERT INTO gt_postgresql1_1.gt_push_db1.employee_performance (employee_id, evaluation_date, rating) VALUES
(1, DATE '2018-02-24', 4),
(1, DATE '2016-12-25', 7),
(1, DATE '2023-04-07', 4),
(3, DATE '2012-11-08', 7),
(3, DATE '2019-09-15', 2),
(3, DATE '2017-06-21', 8),
(3, DATE '2019-07-16', 4),
(3, DATE '2015-10-06', 4),
(3, DATE '2021-01-05', 6),
(3, DATE '2014-10-24', 4);

INSERT INTO gt_postgresql1_1.gt_push_db1.employees (employee_id, department_id, job_title, given_name, family_name, birth_date, hire_date) VALUES
(1, 1, 'Manager', 'Gregory', 'Smith', DATE '1968-04-15', DATE '2014-06-04'),
(2, 1, 'Sales Assistant', 'Owen', 'Rivers', DATE '1988-08-13', DATE '2021-02-05'),
(3, 1, 'Programmer', 'Avram', 'Lawrence', DATE '1969-11-21', DATE '2010-09-29'),
(4, 1, 'Sales Assistant', 'Burton', 'Everett', DATE '2001-12-07', DATE '2016-06-25'),
(5, 1, 'Sales Assistant', 'Cedric', 'Barlow', DATE '1972-02-02', DATE '2012-08-15'),
(6, 2, 'Sales Assistant', 'Jasper', 'Mack', DATE '2002-03-29', DATE '2020-09-13'),
(7, 1, 'Sales Assistant', 'Felicia', 'Robinson', DATE '1973-08-21', DATE '2023-05-14'),
(8, 3, 'Sales Assistant', 'Mason', 'Steele', DATE '1964-05-19', DATE '2019-02-06'),
(9, 3, 'Programmer', 'Bernard', 'Cameron', DATE '1995-08-27', DATE '2018-07-12'),
(10, 2, 'Programmer', 'Chelsea', 'Wade', DATE '2007-01-29', DATE '2016-04-16');

CREATE TABLE gt_postgresql1_1.gt_push_db1.customer (
   custkey bigint NOT NULL,
   name varchar(25) NOT NULL,
   address varchar(40) NOT NULL,
   nationkey bigint NOT NULL,
   phone varchar(15) NOT NULL,
   acctbal decimal(12, 2) NOT NULL,
   mktsegment varchar(10) NOT NULL,
   comment varchar(117) NOT NULL
);

CREATE TABLE gt_postgresql1_1.gt_push_db1.orders (
   orderkey bigint NOT NULL,
   custkey bigint NOT NULL,
   orderstatus varchar(1) NOT NULL,
   totalprice decimal(12, 2) NOT NULL,
   orderdate date NOT NULL,
   orderpriority varchar(15) NOT NULL,
   clerk varchar(15) NOT NULL,
   shippriority integer NOT NULL,
   comment varchar(79) NOT NULL
);

INSERT INTO gt_postgresql1_1.gt_push_db1.customer SELECT * FROM tpch.tiny.customer;

INSERT INTO gt_postgresql1_1.gt_push_db1.orders SELECT * FROM tpch.tiny.orders;