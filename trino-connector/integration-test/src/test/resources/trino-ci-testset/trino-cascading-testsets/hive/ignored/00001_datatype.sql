SHOW CREATE TABLE tb01;

SELECT * FROM tb01 order by f1;

SELECT * FROM test_array_basic;

SELECT int_array, CARDINALITY(int_array) AS array_length FROM test_array_basic;

SELECT elements[1] AS first_element, elements[2] AS second_element FROM test_array_access;

SELECT * FROM test_array_basic WHERE contains(int_array, 2);

SELECT array1, array2, CONCAT(array1, array2) AS concatenated_array FROM test_array_concat;

SELECT unsorted_array, array_sort(unsorted_array) AS sorted_array FROM test_array_sort;

SELECT mixed_array, CARDINALITY(mixed_array) FROM test_array_nulls;

SELECT ARRAY_AGG(val) AS aggregated_array FROM test_array_agg;

SELECT nested_array FROM test_nested_array;