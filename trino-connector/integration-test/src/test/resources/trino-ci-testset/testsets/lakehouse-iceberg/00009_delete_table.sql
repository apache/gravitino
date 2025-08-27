CREATE SCHEMA gt_db2;

-- Test row level deletion
CREATE TABLE gt_db2.tb01 (
    name varchar,
    salary int
);

INSERT INTO gt_db2.tb01(name, salary) VALUES ('bob', 14), ('tom', 12);

SELECT * FROM gt_db2.tb01 ORDER BY name;

DELETE FROM gt_db2.tb01 WHERE salary=14;

SELECT * FROM gt_db2.tb01 ORDER BY name;

DELETE FROM gt_db2.tb01;

SELECT count(*) FROM gt_db2.tb01;

-- Test deletion by partition
CREATE TABLE gt_db2.tb02 (
    name varchar,
    salary int
) with (
    partitioning = ARRAY['name']
);

INSERT INTO gt_db2.tb02(name, salary) VALUES ('bob', 14), ('tom', 12), ('bob', 15);

SELECT * FROM gt_db2.tb02 ORDER BY name;

DELETE FROM gt_db2.tb02 WHERE name='bob';

SELECT * FROM gt_db2.tb02 ORDER BY name;

CREATE TABLE gt_db2.tb03 (
    name varchar,
    salary int
) WITH (format_version='1');

INSERT INTO gt_db2.tb03(name, salary) VALUES ('bob', 14), ('tom', 12);

DELETE FROM gt_db2.tb03 WHERE name='bob';

DROP TABLE gt_db2.tb01;

DROP TABLE gt_db2.tb02;

DROP TABLE gt_db2.tb03;

DROP SCHEMA gt_db2;
