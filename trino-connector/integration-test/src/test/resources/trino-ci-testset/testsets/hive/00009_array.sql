CREATE SCHEMA gt_hive.array_db1;

USE gt_hive.array_db1;

CREATE TABLE test_array_basic (int_array ARRAY(INTEGER));

INSERT INTO test_array_basic VALUES (ARRAY[1, 2, 3]), (ARRAY[4, 5, NULL, 7]), (ARRAY[]);

SELECT * FROM test_array_basic;

SELECT int_array, CARDINALITY(int_array) AS array_length FROM test_array_basic;

CREATE TABLE test_array_access (elements ARRAY(VARCHAR));

INSERT INTO test_array_access VALUES (ARRAY['apple', 'banana', 'cherry']);

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

DROP TABLE gt_hive.array_db1.test_array_basic;

DROP TABLE gt_hive.array_db1.test_array_access;

DROP TABLE gt_hive.array_db1.test_array_concat;

DROP TABLE gt_hive.array_db1.test_array_sort;

DROP TABLE gt_hive.array_db1.test_array_nulls;

DROP TABLE gt_hive.array_db1.test_array_agg;

DROP TABLE gt_hive.array_db1.test_nested_array;

DROP SCHEMA gt_hive.array_db1;