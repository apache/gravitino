CREATE SCHEMA gt_hive.map_db1;

USE gt_hive.map_db1;

CREATE TABLE test_map_nulls (string_map MAP(VARCHAR, VARCHAR));

INSERT INTO test_map_nulls VALUES (MAP(ARRAY['key1'], ARRAY[NULL]));

INSERT INTO test_map_nulls VALUES (MAP(ARRAY[NULL], ARRAY['value1']));

SELECT * FROM test_map_nulls;

INSERT INTO test_map_nulls VALUES (MAP(ARRAY[], ARRAY[]));

SELECT * FROM test_map_nulls ORDER BY cardinality(string_map);

INSERT INTO test_map_nulls VALUES (MAP(ARRAY['dup', 'dup'], ARRAY['value1', 'value2']));

CREATE TABLE test_map_types (int_decimal_map MAP(INTEGER, DECIMAL(10, 2)));

INSERT INTO test_map_types VALUES (MAP(ARRAY[1, 2147483647], ARRAY[12345.67, 99999.99]));

SELECT * FROM test_map_types;

INSERT INTO test_map_nulls VALUES (MAP(ARRAY['k1', 'k2', 'k3'], ARRAY['v1', 'v2', 'v3']));

SELECT element_at(string_map, 'k1') AS key1_value, element_at(string_map, 'k3') AS key3_value FROM test_map_nulls ORDER BY key1_value;

CREATE TABLE test_map_complex (map_of_arrays MAP(VARCHAR, ARRAY(INTEGER)));

INSERT INTO test_map_complex VALUES (MAP(ARRAY['a', 'b'], ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]));

SELECT * FROM test_map_complex;

CREATE TABLE test_map_aggregation (map_data MAP(VARCHAR, INTEGER));

INSERT INTO test_map_aggregation VALUES (MAP(ARRAY['a', 'b'], ARRAY[1, 2])), (MAP(ARRAY['a', 'b'], ARRAY[3, 4]));

SELECT map_data['a'] AS key_a, SUM(map_data['b']) AS sum_b FROM test_map_aggregation GROUP BY map_data['a'] ORDER BY key_a;

DROP TABLE gt_hive.map_db1.test_map_nulls;

DROP TABLE gt_hive.map_db1.test_map_types;

DROP TABLE gt_hive.map_db1.test_map_complex;

DROP TABLE gt_hive.map_db1.test_map_aggregation;

DROP SCHEMA gt_hive.map_db1;