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

SELECT * FROM tb01 order by f1;

SHOW CREATE TABLE tb01;

CREATE TABLE test_array_basic (int_array ARRAY(INTEGER));
INSERT INTO test_array_basic VALUES (ARRAY[1, 2, 3]), (ARRAY[4, 5, NULL, 7]), (ARRAY[]);

SELECT * FROM test_array_basic;

CREATE TABLE test_array_access (elements ARRAY(VARCHAR));
INSERT INTO test_array_access VALUES (ARRAY['apple', 'banana', 'cherry']);

SELECT int_array, CARDINALITY(int_array) AS array_length FROM test_array_basic;

SELECT elements[1] AS first_element, elements[2] AS second_element FROM test_array_access;

SELECT * FROM test_array_basic WHERE contains(int_array, 2);


CREATE TABLE test_array_concat (array1 ARRAY(INTEGER), array2 ARRAY(INTEGER));

INSERT INTO test_array_concat VALUES (ARRAY[1, 2, 3], ARRAY[4, 5]);

SELECT array1, array2, CONCAT(array1, array2) AS concatenated_array FROM test_array_concat;

CREATE TABLE test_array_sort (unsorted_array ARRAY(INTEGER));

INSERT INTO test_array_sort VALUES (ARRAY[3, 1, 2]), (ARRAY[9, 7, 8]);

SELECT unsorted_array, array_sort(unsorted_array) AS sorted_array FROM test_array_sort;

CREATE TABLE test_array_nulls (mixed_array ARRAY(INTEGER));

INSERT INTO test_array_nulls VALUES (ARRAY[1, NULL, 3]), (ARRAY[NULL, NULL]);

SELECT mixed_array, CARDINALITY(mixed_array) FROM test_array_nulls;

CREATE TABLE test_array_agg (val INTEGER);

INSERT INTO test_array_agg VALUES (1), (2), (3), (4);

SELECT ARRAY_AGG(val) AS aggregated_array FROM test_array_agg;

CREATE TABLE test_nested_array (nested_array ARRAY(ARRAY(VARCHAR)));

INSERT INTO test_nested_array VALUES (ARRAY[ARRAY['a', 'b'], ARRAY['c', 'd']]);

SELECT nested_array FROM test_nested_array;

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