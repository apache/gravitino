CREATE SCHEMA gt_ctas_db;

-- Test 1: Basic CTAS with data
CREATE TABLE gt_ctas_db.src_table (
    id integer,
    name varchar,
    value decimal(12, 2)
);

INSERT INTO gt_ctas_db.src_table VALUES (1, 'alice', 10.50), (2, 'bob', 20.75), (3, 'charlie', 30.00);

CREATE TABLE gt_ctas_db.ctas_basic AS SELECT * FROM gt_ctas_db.src_table;

SELECT * FROM gt_ctas_db.ctas_basic ORDER BY id;

-- Test 2: CTAS with column subset and transformation
CREATE TABLE gt_ctas_db.ctas_transform AS SELECT id, upper(name) AS upper_name FROM gt_ctas_db.src_table WHERE id > 1;

SELECT * FROM gt_ctas_db.ctas_transform ORDER BY id;

-- Test 3: CTAS with table properties
CREATE TABLE gt_ctas_db.ctas_with_props
WITH (format = 'ORC')
AS SELECT * FROM gt_ctas_db.src_table;

SELECT * FROM gt_ctas_db.ctas_with_props ORDER BY id;

show create table gt_ctas_db.ctas_with_props;

-- Test 4: CTAS with no data (empty source)
CREATE TABLE gt_ctas_db.ctas_empty AS SELECT * FROM gt_ctas_db.src_table WHERE id < 0;

SELECT count(*) FROM gt_ctas_db.ctas_empty;

-- Test 5: CTAS with partitioning
CREATE TABLE gt_ctas_db.ctas_partitioned
WITH (partitioning = ARRAY['name'])
AS SELECT * FROM gt_ctas_db.src_table;

SELECT * FROM gt_ctas_db.ctas_partitioned ORDER BY id;

show create table gt_ctas_db.ctas_partitioned;

-- Cleanup
DROP TABLE gt_ctas_db.ctas_partitioned;

DROP TABLE gt_ctas_db.ctas_empty;

DROP TABLE gt_ctas_db.ctas_with_props;

DROP TABLE gt_ctas_db.ctas_transform;

DROP TABLE gt_ctas_db.ctas_basic;

DROP TABLE gt_ctas_db.src_table;

DROP SCHEMA gt_ctas_db;
