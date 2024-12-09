CREATE SCHEMA gt_hive.gt_row_db1;

USE gt_hive.gt_row_db1;

CREATE TABLE tb01 (person ROW(id INTEGER, name VARCHAR));

CREATE TABLE source_tb1 (id INTEGER, name VARCHAR);

INSERT INTO source_tb1 VALUES (1, 'Alice'), (2, NULL);

INSERT INTO tb01 SELECT ROW(id, name) FROM source_tb1;

SELECT * FROM tb01 ORDER BY person.id;

INSERT INTO source_tb1 VALUES (3, 'Bob');

INSERT INTO tb01 SELECT ROW(id, name) FROM source_tb1;

SELECT person.id AS person_id, person.name AS person_name FROM tb01 ORDER BY person.id;

CREATE TABLE tb02 (
    person ROW(id INTEGER, name VARCHAR, address ROW(street VARCHAR, city VARCHAR))
);

CREATE TABLE source_tb2 (id INTEGER, name VARCHAR, street VARCHAR, city VARCHAR);

INSERT INTO source_tb2 VALUES (1, 'Alice', '123 Elm St', 'Springfield');

INSERT INTO tb02 SELECT ROW(id, name, ROW(street, city)) FROM source_tb2;

SELECT person.address.city AS city FROM tb02;

CREATE TABLE tb03 (
    data ROW(int_val INTEGER, str_val VARCHAR, arr_val ARRAY(INTEGER), map_val MAP(VARCHAR, INTEGER))
);

CREATE TABLE source_tb3 (int_val INTEGER, str_val VARCHAR, arr_val ARRAY(INTEGER), map_val MAP(VARCHAR, INTEGER));

INSERT INTO source_tb3 VALUES (100, 'text', ARRAY[1, 2, 3], MAP(ARRAY['a', 'b'], ARRAY[10, 20]));

INSERT INTO tb03 SELECT ROW(int_val, str_val, arr_val, map_val) FROM source_tb3;

SELECT * FROM tb03;

INSERT INTO source_tb1 VALUES (NULL, NULL);

INSERT INTO tb01 SELECT ROW(id, name) FROM source_tb1;

SELECT * FROM tb01 ORDER BY person.id;

CREATE TABLE tb04 (
    row_array ARRAY(ROW(id INTEGER, name VARCHAR)),
    row_map MAP(VARCHAR, ROW(age INTEGER, city VARCHAR))
);

CREATE TABLE source_tb5 (id INTEGER, name VARCHAR, age INTEGER, city VARCHAR);

INSERT INTO source_tb5 VALUES (1, 'Alice', 30, 'NY'), (2, 'Bob', 40, 'LA');

INSERT INTO tb04 SELECT ARRAY[ROW(id, name)], MAP(ARRAY['person1'], ARRAY[ROW(age, city)]) FROM source_tb5;

INSERT INTO tb04
SELECT ARRAY_AGG(ROW(id, name)), MAP(ARRAY_AGG(person_key), ARRAY_AGG(ROW(age, city)))
FROM (
    SELECT id, name, age, city, CONCAT('person', CAST(ROW_NUMBER() OVER() AS VARCHAR)) AS person_key
    FROM source_tb5
) subquery;

INSERT INTO source_tb1 VALUES (1, 'Alice'), (1, 'Alice'), (2, 'Bob');

INSERT INTO tb01 SELECT ROW(id, name) FROM source_tb1;

SELECT person.id, COUNT(*) FROM tb01 GROUP BY person.id ORDER BY person.id;

DROP TABLE gt_hive.gt_row_db1.tb01;

DROP TABLE gt_hive.gt_row_db1.source_tb1;

DROP TABLE gt_hive.gt_row_db1.tb02;

DROP TABLE gt_hive.gt_row_db1.tb03;

DROP TABLE gt_hive.gt_row_db1.source_tb2;

DROP TABLE gt_hive.gt_row_db1.source_tb3;

DROP TABLE gt_hive.gt_row_db1.tb04;

DROP TABLE gt_hive.gt_row_db1.source_tb5;

DROP SCHEMA gt_hive.gt_row_db1;