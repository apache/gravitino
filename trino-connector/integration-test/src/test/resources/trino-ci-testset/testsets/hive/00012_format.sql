CREATE SCHEMA gt_hive.gt_format_db1;

USE gt_hive.gt_format_db1;

CREATE TABLE storage_formats_orc (
c_boolean boolean,
c_tinyint tinyint,
c_smallint smallint,
c_int  integer,
c_bigint bigint,
c_real real,
c_double double,
c_decimal_10_0 decimal(10,0),
c_decimal_10_2 decimal(10,2),
c_decimal_38_5 decimal(38,5),
c_char char(10),
c_varchar varchar(10),
c_string varchar,
c_binary varbinary,
c_date date,
c_timestamp timestamp,
c_array array(integer),
c_map map(varchar, varchar),
c_row row(f1 integer, f2 varchar)
) WITH (format='ORC');

INSERT INTO storage_formats_orc
VALUES (true,127,32767,2147483647,9223372036854775807,123.345,234.567,346,12345678.91,1234567890123456789012.34567,'ala ma    ','ala ma kot','ala ma kota',X'62696e61727920636f6e74656e74',DATE '2024-11-11',TIMESTAMP '2024-11-11 12:15:35.123',ARRAY[1, 2, 3],MAP(ARRAY['foo'], ARRAY['bar']),ROW(42, 'Trino'));

SELECT * FROM storage_formats_orc;

CREATE TABLE storage_formats_textfile (
c_boolean boolean,
c_tinyint tinyint,
c_smallint smallint,
c_int  integer,
c_bigint bigint,
c_real real,
c_double double,
c_decimal_10_0 decimal(10,0),
c_decimal_10_2 decimal(10,2),
c_decimal_38_5 decimal(38,5),
c_char char(10),
c_varchar varchar(10),
c_string varchar,
c_binary varbinary,
c_date date,
c_timestamp timestamp,
c_array array(integer),
c_map map(varchar, varchar),
c_row row(f1 integer, f2 varchar)
) WITH (format='TextFile');

INSERT INTO storage_formats_textfile
VALUES (true,127,32767,2147483647,9223372036854775807,123.345,234.567,346,12345678.91,1234567890123456789012.34567,'ala ma    ','ala ma kot','ala ma kota',X'62696e61727920636f6e74656e74',DATE '2024-11-11',TIMESTAMP '2024-11-11 12:15:35.123',ARRAY[1, 2, 3],MAP(ARRAY['foo'], ARRAY['bar']),ROW(42, 'Trino'));

SELECT * FROM storage_formats_textfile;

CREATE TABLE storage_formats_parquet (
c_boolean boolean,
c_tinyint tinyint,
c_smallint smallint,
c_int  integer,
c_bigint bigint,
c_real real,
c_double double,
c_decimal_10_0 decimal(10,0),
c_decimal_10_2 decimal(10,2),
c_decimal_38_5 decimal(38,5),
c_char char(10),
c_varchar varchar(10),
c_string varchar,
c_binary varbinary,
c_date date,
c_timestamp timestamp,
c_array array(integer),
c_map map(varchar, varchar),
c_row row(f1 integer, f2 varchar)
) WITH (format='PARQUET');

INSERT INTO storage_formats_parquet VALUES (true,127,32767,2147483647,9223372036854775807,123.345,234.567,346,12345678.91,1234567890123456789012.34567,'ala ma    ','ala ma kot','ala ma kota',X'62696e61727920636f6e74656e74',DATE '2024-11-11',TIMESTAMP '2024-11-11 12:15:35.123',ARRAY[1, 2, 3],MAP(ARRAY['foo'], ARRAY['bar']),ROW(42, 'Trino'));

SELECT * FROM storage_formats_parquet;

CREATE TABLE storage_formats_rcfile (
c_boolean boolean,
c_tinyint tinyint,
c_smallint smallint,
c_int  integer,
c_bigint bigint,
c_real real,
c_double double,
c_decimal_10_0 decimal(10,0),
c_decimal_10_2 decimal(10,2),
c_decimal_38_5 decimal(38,5),
c_char char(10),
c_varchar varchar(10),
c_string varchar,
c_binary varbinary,
c_date date,
c_timestamp timestamp,
c_array array(integer),
c_map map(varchar, varchar),
c_row row(f1 integer, f2 varchar)
) WITH (format='RCFile');

INSERT INTO storage_formats_rcfile VALUES (true,127,32767,2147483647,9223372036854775807,123.345,234.567,346,12345678.91,1234567890123456789012.34567,'ala ma    ','ala ma kot','ala ma kota',X'62696e61727920636f6e74656e74',DATE '2024-11-11',TIMESTAMP '2024-11-11 12:15:35.123',ARRAY[1, 2, 3],MAP(ARRAY['foo'], ARRAY['bar']),ROW(42, 'Trino'));

SELECT * FROM storage_formats_rcfile;

CREATE TABLE storage_formats_sequencefile (
c_boolean boolean,
c_tinyint tinyint,
c_smallint smallint,
c_int  integer,
c_bigint bigint,
c_real real,
c_double double,
c_decimal_10_0 decimal(10,0),
c_decimal_10_2 decimal(10,2),
c_decimal_38_5 decimal(38,5),
c_char char(10),
c_varchar varchar(10),
c_string varchar,
c_binary varbinary,
c_date date,
c_timestamp timestamp,
c_array array(integer),
c_map map(varchar, varchar),
c_row row(f1 integer, f2 varchar)
) WITH (format='SequenceFile');

INSERT INTO storage_formats_sequencefile VALUES (true,127,32767,2147483647,9223372036854775807,123.345,234.567,346,12345678.91,1234567890123456789012.34567,'ala ma    ','ala ma kot','ala ma kota',X'62696e61727920636f6e74656e74',DATE '2024-11-11',TIMESTAMP '2024-11-11 12:15:35.123',ARRAY[1, 2, 3],MAP(ARRAY['foo'], ARRAY['bar']),ROW(42, 'Trino')
);

SELECT * FROM storage_formats_sequencefile;

CREATE TABLE storage_formats_avro (
c_boolean boolean,
c_tinyint integer,
c_smallint integer,
c_int  integer,
c_bigint bigint,
c_real real,
c_double double,
c_decimal_10_0 decimal(10,0),
c_decimal_10_2 decimal(10,2),
c_decimal_38_5 decimal(38,5),
c_char char(10),
c_varchar varchar(10),
c_string varchar,
c_binary varbinary,
c_date date,
c_timestamp timestamp,
c_array array(integer),
c_map map(varchar, varchar),
c_row row(f1 integer, f2 varchar)
) WITH (format='AVRO');

INSERT INTO storage_formats_avro VALUES (true,127,32767,2147483647,9223372036854775807,123.345,234.567,346,12345678.91,1234567890123456789012.34567,'ala ma    ','ala ma kot','ala ma kota',X'62696e61727920636f6e74656e74',DATE '2024-11-11',TIMESTAMP '2024-11-11 12:15:35.123',ARRAY[1, 2, 3],MAP(ARRAY['foo'], ARRAY['bar']),ROW(42, 'Trino'));

SELECT * FROM storage_formats_avro;

DROP TABLE gt_hive.gt_format_db1.storage_formats_orc;

DROP TABLE gt_hive.gt_format_db1.storage_formats_textfile;

DROP TABLE gt_hive.gt_format_db1.storage_formats_parquet;

DROP TABLE gt_hive.gt_format_db1.storage_formats_rcfile;

DROP TABLE gt_hive.gt_format_db1.storage_formats_sequencefile;

DROP TABLE gt_hive.gt_format_db1.storage_formats_avro;

DROP SCHEMA gt_hive.gt_format_db1;
