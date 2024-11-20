call gravitino.system.create_catalog(
    'gt_hive1',
    'hive',
     MAP(
        ARRAY['metastore.uris', 'cloud.region-code', 'cloud.trino.connection-url', 'cloud.trino.connection-user', 'cloud.trino.connection-password'],
        ARRAY['${hive_uri}', 'c2', '${trino_remote_jdbc_uri}', 'admin', '']
    )
);

call gravitino.system.create_catalog(
    'gt_hive1_1',
    'hive',
    map(
        array['metastore.uris'],
        array['${hive_uri}']
    )
);

CREATE SCHEMA gt_hive1_1.gt_datatype;
USE gt_hive1_1.gt_datatype;

CREATE TABLE tb01 (
    f1 VARCHAR(200),
    f2 CHAR(20),
    f3 VARBINARY,
    f4 DECIMAL(10, 3),
    f5 REAL,
    f6 DOUBLE,
    f7 BOOLEAN,
    f8 TINYINT,
    f9 SMALLINT,
    f10 INT,
    f11 INTEGER,
    f12 BIGINT,
    f13 DATE,
    f15 TIMESTAMP
);

INSERT INTO tb01 (f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f15)
VALUES ('Sample text 1', 'Text1', x'65', 123.456, 7.89, 12.34, false, 1, 100, 1000, 1000, 100000, DATE '2024-01-01', TIMESTAMP '2024-01-01 08:00:00');

INSERT INTO tb01 (f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f15)
VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

CREATE TABLE tb02 (name char(255));
INSERT INTO tb02 (name) VALUES ('Apache Gravitino is a high-performance, geo-distributed, and federated metadata lake. It manages metadata directly in different sources, types, and regions, providing users with unified metadata access for data and AI assets.');

CREATE TABLE tb03 (name char);
INSERT INTO tb03 VALUES ('a');

CREATE TABLE tb04 (name varchar);
INSERT INTO tb04 VALUES ('test abc');

CREATE TABLE test_decimal_bounds (amount DECIMAL(10, 2));

INSERT INTO test_decimal_bounds VALUES (12345.67), (-9999999.99), (0.01);

CREATE TABLE test_decimal_aggregation (value DECIMAL(12, 3));

INSERT INTO test_decimal_aggregation VALUES (1234.567), (8901.234), (567.890);

CREATE TABLE test_decimal_arithmetic (val1 DECIMAL(5, 2), val2 DECIMAL(4, 1));

INSERT INTO test_decimal_arithmetic VALUES (123.45,10.1);

CREATE TABLE test_decimal_max_min (max_min_val DECIMAL(18, 4));

INSERT INTO test_decimal_max_min VALUES (99999999999999.9999);

INSERT INTO test_decimal_max_min VALUES (-99999999999999.9999);

CREATE TABLE test_decimal_nulls (nullable_val DECIMAL(8, 2));

INSERT INTO test_decimal_nulls VALUES (NULL), (123.45), (NULL);
