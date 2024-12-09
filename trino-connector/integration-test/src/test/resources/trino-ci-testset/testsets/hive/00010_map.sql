CREATE SCHEMA gt_hive.gt_map_db1;

USE gt_hive.gt_map_db1;

CREATE TABLE tb01 (string_map MAP(VARCHAR, VARCHAR));

INSERT INTO tb01 VALUES (MAP(ARRAY['key1'], ARRAY[NULL]));

INSERT INTO tb01 VALUES (MAP(ARRAY[NULL], ARRAY['value1']));

SELECT * FROM tb01;

INSERT INTO tb01 VALUES (MAP(ARRAY[], ARRAY[]));

SELECT * FROM tb01 ORDER BY cardinality(string_map);

INSERT INTO tb01 VALUES (MAP(ARRAY['dup', 'dup'], ARRAY['value1', 'value2']));

CREATE TABLE tb02 (int_decimal_map MAP(INTEGER, DECIMAL(10, 2)));

INSERT INTO tb02 VALUES (MAP(ARRAY[1, 2147483647], ARRAY[12345.67, 99999.99]));

SELECT * FROM tb02;

INSERT INTO tb01 VALUES (MAP(ARRAY['k1', 'k2', 'k3'], ARRAY['v1', 'v2', 'v3']));

SELECT element_at(string_map, 'k1') AS key1_value, element_at(string_map, 'k3') AS key3_value FROM tb01 ORDER BY key1_value;

CREATE TABLE tb03 (map_of_arrays MAP(VARCHAR, ARRAY(INTEGER)));

INSERT INTO tb03 VALUES (MAP(ARRAY['a', 'b'], ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]));

SELECT * FROM tb03;

CREATE TABLE tb04 (map_data MAP(VARCHAR, INTEGER));

INSERT INTO tb04 VALUES (MAP(ARRAY['a', 'b'], ARRAY[1, 2])), (MAP(ARRAY['a', 'b'], ARRAY[3, 4]));

SELECT map_data['a'] AS key_a, SUM(map_data['b']) AS sum_b FROM tb04 GROUP BY map_data['a'] ORDER BY key_a;

DROP TABLE gt_hive.gt_map_db1.tb01;

DROP TABLE gt_hive.gt_map_db1.tb02;

DROP TABLE gt_hive.gt_map_db1.tb03;

DROP TABLE gt_hive.gt_map_db1.tb04;

DROP SCHEMA gt_hive.gt_map_db1;