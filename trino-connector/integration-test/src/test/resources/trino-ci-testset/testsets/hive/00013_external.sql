CREATE SCHEMA gt_hive_conn.gt_external_db2;

USE gt_hive_conn.gt_external_db2;

CREATE TABLE ext_tb01 (
    c_tinyint tinyint,
    c_smallint smallint,
    c_int integer,
    c_bigint bigint,
    c_float real,
    c_double double,
    c_decimal decimal(10, 3),
    c_decimal_w_params decimal(10, 5),
    c_timestamp timestamp(3),
    c_date date,
    c_string varchar,
    c_varchar varchar(10),
    c_char char(10),
    c_boolean boolean,
    c_binary varbinary
) WITH (format='TEXTFILE',external_location='file:///tmp/trino/data/hive/types',textfile_field_separator='|') ;

USE gt_hive.gt_external_db2;

SELECT * FROM gt_hive.gt_external_db2.ext_tb01;

CREATE SCHEMA gt_hive.gt_external_db1;

USE gt_hive.gt_external_db1;

CREATE TABLE ext_tb02 (
    col  integer
) WITH (format='parquet',location='file:///tmp/trino/data/hive/formats/parquet',table_type ='EXTERNAL_TABLE');

SELECT * FROM ext_tb02;

CREATE TABLE ext_tb03 (
    col  integer
) WITH (format='orc',location='file:///tmp/trino/data/hive/formats/orc',table_type ='EXTERNAL_TABLE');

SELECT * FROM ext_tb03;

CREATE TABLE ext_tb04 (
    col  integer
) WITH (format='avro',location='file:///tmp/trino/data/hive/formats/avro',table_type ='EXTERNAL_TABLE');

SELECT * FROM ext_tb04;

CREATE TABLE ext_tb05 (
    col  integer
) WITH (format='rcfile',location='file:///tmp/trino/data/hive/formats/rcfile',table_type ='EXTERNAL_TABLE');

SELECT * FROM ext_tb05;

CREATE TABLE ext_tb06 (
    col  integer
) WITH (format='textfile',location='file:///tmp/trino/data/hive/formats/textfile',table_type ='EXTERNAL_TABLE');

SELECT * FROM ext_tb06;