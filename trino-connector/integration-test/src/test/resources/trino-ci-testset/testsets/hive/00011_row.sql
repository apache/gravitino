CREATE SCHEMA gt_hive.row_db1;

USE gt_hive.row_db1;

CREATE TABLE test_row_basic (person ROW(id INTEGER, name VARCHAR));

CREATE TABLE source_tb1 (id INTEGER, name VARCHAR);

INSERT INTO source_tb1 VALUES (1, 'Alice'), (2, NULL);

INSERT INTO test_row_basic SELECT ROW(id, name) FROM source_tb1;

SELECT * FROM test_row_basic ORDER BY person.id;

INSERT INTO source_tb1 VALUES (3, 'Bob');

INSERT INTO test_row_basic SELECT ROW(id, name) FROM source_tb1;

SELECT person.id AS person_id, person.name AS person_name FROM test_row_basic ORDER BY person.id;

CREATE TABLE test_nested_row (
    person ROW(id INTEGER, name VARCHAR, address ROW(street VARCHAR, city VARCHAR))
);

CREATE TABLE source_tb2 (id INTEGER, name VARCHAR, street VARCHAR, city VARCHAR);

INSERT INTO source_tb2 VALUES (1, 'Alice', '123 Elm St', 'Springfield');

INSERT INTO test_nested_row SELECT ROW(id, name, ROW(street, city)) FROM source_tb2;

SELECT person.address.city AS city FROM test_nested_row;

CREATE TABLE test_mixed_row (
    data ROW(int_val INTEGER, str_val VARCHAR, arr_val ARRAY(INTEGER), map_val MAP(VARCHAR, INTEGER))
);

CREATE TABLE source_tb3 (int_val INTEGER, str_val VARCHAR, arr_val ARRAY(INTEGER), map_val MAP(VARCHAR, INTEGER));

INSERT INTO source_tb3 VALUES (100, 'text', ARRAY[1, 2, 3], MAP(ARRAY['a', 'b'], ARRAY[10, 20]));

INSERT INTO test_mixed_row SELECT ROW(int_val, str_val, arr_val, map_val) FROM source_tb3;

SELECT * FROM test_mixed_row;

INSERT INTO source_tb1 VALUES (NULL, NULL);

INSERT INTO test_row_basic SELECT ROW(id, name) FROM source_tb1;

SELECT * FROM test_row_basic ORDER BY person.id;

CREATE TABLE test_row_in_array_map (
    row_array ARRAY(ROW(id INTEGER, name VARCHAR)),
    row_map MAP(VARCHAR, ROW(age INTEGER, city VARCHAR))
);

CREATE TABLE source_tb5 (id INTEGER, name VARCHAR, age INTEGER, city VARCHAR);

INSERT INTO source_tb5 VALUES (1, 'Alice', 30, 'NY'), (2, 'Bob', 40, 'LA');

INSERT INTO test_row_in_array_map SELECT ARRAY[ROW(id, name)], MAP(ARRAY['person1'], ARRAY[ROW(age, city)]) FROM source_tb5;

INSERT INTO test_row_in_array_map
SELECT ARRAY_AGG(ROW(id, name)), MAP(ARRAY_AGG(person_key), ARRAY_AGG(ROW(age, city)))
FROM (
    SELECT id, name, age, city, CONCAT('person', CAST(ROW_NUMBER() OVER() AS VARCHAR)) AS person_key
    FROM source_tb5
) subquery;

INSERT INTO source_tb1 VALUES (1, 'Alice'), (1, 'Alice'), (2, 'Bob');

INSERT INTO test_row_basic SELECT ROW(id, name) FROM source_tb1;

SELECT person.id, COUNT(*) FROM test_row_basic GROUP BY person.id ORDER BY person.id;

DROP TABLE gt_hive.row_db1.test_row_basic;

DROP TABLE gt_hive.row_db1.source_tb1;

DROP TABLE gt_hive.row_db1.test_nested_row;

DROP TABLE gt_hive.row_db1.test_mixed_row;

DROP TABLE gt_hive.row_db1.source_tb2;

DROP TABLE gt_hive.row_db1.source_tb3;

DROP TABLE gt_hive.row_db1.test_row_in_array_map;

DROP TABLE gt_hive.row_db1.source_tb5;

DROP SCHEMA gt_hive.row_db1;