CREATE SCHEMA gt_db2;

CREATE TABLE gt_db2.tb01 (
    name varchar,
    salary int
);

INSERT INTO gt_db2.tb01(name, salary) VALUES ('bob', 14), ('tom', 12);

SELECT * FROM gt_db2.tb01 ORDER BY name;

UPDATE gt_db2.tb01 SET name='bob_update' WHERE salary=14;

SELECT * FROM gt_db2.tb01 ORDER BY name;

CREATE TABLE gt_db2.tb02 (
    name varchar,
    salary int
) WITH (format_version='1');

INSERT INTO gt_db2.tb02(name, salary) VALUES ('bob', 14), ('tom', 12);

UPDATE gt_db2.tb02 SET name='bob_update' WHERE salary=14;

DROP TABLE gt_db2.tb01;

DROP TABLE gt_db2.tb02;

DROP SCHEMA gt_db2;
