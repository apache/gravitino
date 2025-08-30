CREATE SCHEMA gt_postgresql.gt_db1;

CREATE TABLE gt_postgresql.gt_db1.tb01 (
    name varchar(200),
    salary int
);

INSERT INTO gt_postgresql.gt_db1.tb01(name, salary) VALUES ('bob', 14), ('tom', 12), ('nancy', 15), ('hoho', 16);

SELECT * FROM gt_postgresql.gt_db1.tb01 ORDER BY name;

DELETE FROM gt_postgresql.gt_db1.tb01 WHERE salary=14;

SELECT * FROM gt_postgresql.gt_db1.tb01 ORDER BY name;

DELETE FROM gt_postgresql.gt_db1.tb01 WHERE salary<16;

SELECT * FROM gt_postgresql.gt_db1.tb01 ORDER BY name;

DELETE FROM gt_postgresql.gt_db1.tb01;

SELECT count(*) FROM gt_postgresql.gt_db1.tb01;

DROP TABLE gt_postgresql.gt_db1.tb01;

DROP SCHEMA gt_postgresql.gt_db1;
