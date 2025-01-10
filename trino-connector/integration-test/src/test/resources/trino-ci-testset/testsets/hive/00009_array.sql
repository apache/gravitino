CREATE SCHEMA gt_hive.gt_array_db1;

USE gt_hive.gt_array_db1;

CREATE TABLE tb01 (int_array ARRAY(INTEGER));

INSERT INTO tb01 VALUES (ARRAY[1, 2, 3]), (ARRAY[4, 5, NULL, 7]), (ARRAY[]);

SELECT * FROM tb01;

SELECT int_array, CARDINALITY(int_array) AS array_length FROM tb01;

CREATE TABLE tb02 (elements ARRAY(VARCHAR));

INSERT INTO tb02 VALUES (ARRAY['apple', 'banana', 'cherry']);

SELECT elements[1] AS first_element, elements[2] AS second_element FROM tb02;

SELECT * FROM tb01 WHERE contains(int_array, 2);

CREATE TABLE tb03 (array1 ARRAY(INTEGER), array2 ARRAY(INTEGER));

INSERT INTO tb03 VALUES (ARRAY[1, 2, 3], ARRAY[4, 5]);

SELECT array1, array2, CONCAT(array1, array2) AS concatenated_array FROM tb03;

CREATE TABLE tb04 (unsorted_array ARRAY(INTEGER));

INSERT INTO tb04 VALUES (ARRAY[3, 1, 2]), (ARRAY[9, 7, 8]);

SELECT unsorted_array, array_sort(unsorted_array) AS sorted_array FROM tb04;

CREATE TABLE tb05 (mixed_array ARRAY(INTEGER));

INSERT INTO tb05 VALUES (ARRAY[1, NULL, 3]), (ARRAY[NULL, NULL]);

SELECT mixed_array, CARDINALITY(mixed_array) FROM tb05;

CREATE TABLE tb06 (val INTEGER);

INSERT INTO tb06 VALUES (1), (2), (3), (4);

SELECT ARRAY_AGG(val) AS tb07 FROM tb06;

CREATE TABLE tb08 (nested_array ARRAY(ARRAY(VARCHAR)));

INSERT INTO tb08 VALUES (ARRAY[ARRAY['a', 'b'], ARRAY['c', 'd']]);

SELECT nested_array FROM tb08;

DROP TABLE gt_hive.gt_array_db1.tb01;

DROP TABLE gt_hive.gt_array_db1.tb02;

DROP TABLE gt_hive.gt_array_db1.tb03;

DROP TABLE gt_hive.gt_array_db1.tb04;

DROP TABLE gt_hive.gt_array_db1.tb05;

DROP TABLE gt_hive.gt_array_db1.tb06;

DROP TABLE gt_hive.gt_array_db1.tb08;

DROP SCHEMA gt_hive.gt_array_db1;