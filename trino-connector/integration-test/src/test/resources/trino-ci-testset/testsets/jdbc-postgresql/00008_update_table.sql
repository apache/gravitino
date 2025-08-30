CREATE SCHEMA gt_postgresql.gt_db1;

CREATE TABLE gt_postgresql.gt_db1.tb01 (
    name varchar(200),
    salary int
);

INSERT INTO gt_postgresql.gt_db1.tb01(name, salary) VALUES ('bob', 14), ('tom', 12);

SELECT * FROM gt_postgresql.gt_db1.tb01 ORDER BY name;

UPDATE gt_postgresql.gt_db1.tb01 SET name='bob_update' WHERE salary=14;

SELECT * FROM gt_postgresql.gt_db1.tb01 ORDER BY name;

UPDATE gt_postgresql.gt_db1.tb01 SET name='nancy' WHERE salary < 15;

SELECT * FROM gt_postgresql.gt_db1.tb01 ORDER BY salary;

DROP TABLE gt_postgresql.gt_db1.tb01;

DROP SCHEMA gt_postgresql.gt_db1;
