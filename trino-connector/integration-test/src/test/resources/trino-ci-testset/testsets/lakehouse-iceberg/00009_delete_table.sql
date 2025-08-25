CREATE SCHEMA gt_db2;

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

DROP TABLE gt_db2.tb01;

DROP SCHEMA gt_db2;
