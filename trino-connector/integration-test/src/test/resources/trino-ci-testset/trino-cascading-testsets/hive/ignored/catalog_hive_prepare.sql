CREATE TABLE test_array_basic (int_array ARRAY(INTEGER));

INSERT INTO test_array_basic VALUES (ARRAY[1, 2, 3]), (ARRAY[4, 5, NULL, 7]), (ARRAY[]);

CREATE TABLE test_array_access (elements ARRAY(VARCHAR));

INSERT INTO test_array_access VALUES (ARRAY['apple', 'banana', 'cherry']);

CREATE TABLE test_array_concat (array1 ARRAY(INTEGER), array2 ARRAY(INTEGER));

INSERT INTO test_array_concat VALUES (ARRAY[1, 2, 3], ARRAY[4, 5]);

CREATE TABLE test_array_sort (unsorted_array ARRAY(INTEGER));

INSERT INTO test_array_sort VALUES (ARRAY[3, 1, 2]), (ARRAY[9, 7, 8]);

CREATE TABLE test_array_nulls (mixed_array ARRAY(INTEGER));

INSERT INTO test_array_nulls VALUES (ARRAY[1, NULL, 3]), (ARRAY[NULL, NULL]);

CREATE TABLE test_array_agg (val INTEGER);

INSERT INTO test_array_agg VALUES (1), (2), (3), (4);

CREATE TABLE test_nested_array (nested_array ARRAY(ARRAY(VARCHAR)));

INSERT INTO test_nested_array VALUES (ARRAY[ARRAY['a', 'b'], ARRAY['c', 'd']]);


CREATE SCHEMA gt_hive1_1.gt_format_db1;
USE gt_hive1_1.gt_format_db1;

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
c_timestamp timestamp
) WITH (format='ORC');

INSERT INTO storage_formats_orc
VALUES (true,127,32767,2147483647,9223372036854775807,123.345,234.567,346,12345678.91,1234567890123456789012.34567,'ala ma    ','ala ma kot','ala ma kota',X'62696e61727920636f6e74656e74',DATE '2024-11-11',TIMESTAMP '2024-11-11 12:15:35.123');
