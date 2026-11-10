CREATE SCHEMA gt_postgresql.gt_ctas_db;

-- Test 1: Basic CTAS with data
CREATE TABLE gt_postgresql.gt_ctas_db.src_table (
    id int,
    name varchar,
    value decimal(12, 2)
);

INSERT INTO gt_postgresql.gt_ctas_db.src_table VALUES (1, 'alice', 10.50), (2, 'bob', 20.75), (3, 'charlie', 30.00);

CREATE TABLE gt_postgresql.gt_ctas_db.ctas_basic AS SELECT * FROM gt_postgresql.gt_ctas_db.src_table;

SELECT * FROM gt_postgresql.gt_ctas_db.ctas_basic ORDER BY id;

-- Test 2: CTAS with column subset and transformation
CREATE TABLE gt_postgresql.gt_ctas_db.ctas_transform AS SELECT id, upper(name) AS upper_name FROM gt_postgresql.gt_ctas_db.src_table WHERE id > 1;

SELECT * FROM gt_postgresql.gt_ctas_db.ctas_transform ORDER BY id;

-- Test 3: CTAS with no data (empty source)
CREATE TABLE gt_postgresql.gt_ctas_db.ctas_empty AS SELECT * FROM gt_postgresql.gt_ctas_db.src_table WHERE id < 0;

SELECT count(*) FROM gt_postgresql.gt_ctas_db.ctas_empty;

-- Cleanup
DROP TABLE gt_postgresql.gt_ctas_db.ctas_empty;

DROP TABLE gt_postgresql.gt_ctas_db.ctas_transform;

DROP TABLE gt_postgresql.gt_ctas_db.ctas_basic;

DROP TABLE gt_postgresql.gt_ctas_db.src_table;

DROP SCHEMA gt_postgresql.gt_ctas_db;
