call gravitino.system.create_catalog(
    'gt_mysql1',
    'jdbc-mysql',
    map(
        array['jdbc-url', 'jdbc-user', 'jdbc-password', 'jdbc-driver', 'trino.bypass.join-pushdown.strategy', 'cloud.region-code', 'cloud.trino.connection-url', 'cloud.trino.connection-user', 'cloud.trino.connection-password'],
        array['${mysql_uri}/?useSSL=false', 'trino', 'ds123', 'com.mysql.cj.jdbc.Driver', 'EAGER','c2', '${trino_remote_jdbc_uri}', 'admin', '']
    )
);

call gravitino.system.create_catalog(
    'gt_mysql1_1',
    'jdbc-mysql',
    map(
        array['jdbc-url', 'jdbc-user', 'jdbc-password', 'jdbc-driver', 'trino.bypass.join-pushdown.strategy'],
        array['${mysql_uri}/?useSSL=false', 'trino', 'ds123', 'com.mysql.cj.jdbc.Driver', 'EAGER']
    )
);

CREATE SCHEMA gt_mysql1_1.gt_db1;

USE gt_mysql1_1.gt_db1;

-- Unsupported Type: BOOLEAN
CREATE TABLE tb01 (
    f1 VARCHAR(200),
    f2 CHAR(20),
    f3 VARBINARY,
    f4 DECIMAL(10, 3),
    f5 REAL,
    f6 DOUBLE,
    f8 TINYINT,
    f9 SMALLINT,
    f10 INT,
    f11 INTEGER,
    f12 BIGINT,
    f13 DATE,
    f14 TIME,
    f15 TIMESTAMP,
    f16 TIMESTAMP WITH TIME ZONE
);

INSERT INTO tb01 (f1, f2, f3, f4, f5, f6, f8, f9, f10, f11, f12, f13, f14, f15, f16)
VALUES ('Sample text 1', 'Text1', x'65', 123.456, 7.89, 12.34, 1, 100, 1000, 1000, 100000, DATE '2024-01-01',
        TIME '08:00:00', TIMESTAMP '2024-01-01 08:00:00', TIMESTAMP '2024-01-01 08:00:00 UTC');

INSERT INTO tb01 (f1, f2, f3, f4, f5, f6, f8, f9, f10, f11, f12, f13, f14, f15, f16)
VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULl);

CREATE TABLE tb02 (
    f1 VARCHAR(200) NOT NULL ,
    f2 CHAR(20) NOT NULL ,
    f3 VARBINARY NOT NULL ,
    f4 DECIMAL(10, 3) NOT NULL ,
    f5 REAL NOT NULL ,
    f6 DOUBLE NOT NULL ,
    f8 TINYINT NOT NULL ,
    f9 SMALLINT NOT NULL ,
    f10 INT NOT NULL ,
    f11 INTEGER NOT NULL ,
    f12 BIGINT NOT NULL ,
    f13 DATE NOT NULL ,
    f14 TIME NOT NULL ,
    f15 TIMESTAMP NOT NULL,
    f16 TIMESTAMP WITH TIME ZONE NOT NULL
);

INSERT INTO tb02 (f1, f2, f3, f4, f5, f6, f8, f9, f10, f11, f12, f13, f14, f15, f16)
VALUES ('Sample text 1', 'Text1', x'65', 123.456, 7.89, 12.34, 1, 100, 1000, 1000, 100000, DATE '2024-01-01',
        TIME '08:00:00', TIMESTAMP '2024-01-01 08:00:00', TIMESTAMP '2024-01-01 08:00:00 UTC');

CREATE TABLE tb03 (id int, name char(20));

CREATE TABLE tb04 (id int, name char(255));

CREATE TABLE tb05 (id int, name varchar(250));

CREATE TABLE tb06 (id int, name varchar(256));

CREATE TABLE tb07 (id int, name char);

CREATE TABLE tb08 (id int, name varchar);


CREATE TABLE customer (
   custkey bigint NOT NULL,
   name varchar(25) NOT NULL,
   address varchar(40) NOT NULL,
   nationkey bigint NOT NULL,
   phone varchar(15) NOT NULL,
   acctbal decimal(12, 2) NOT NULL,
   mktsegment varchar(10) NOT NULL,
   comment varchar(117) NOT NULL
);

CREATE TABLE orders (
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

insert into customer select * from tpch.tiny.customer;

insert into orders select * from tpch.tiny.orders;