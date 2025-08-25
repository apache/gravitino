CREATE SCHEMA gt_mysql.gt_db1;

CREATE TABLE gt_mysql.gt_db1.tb01 (
    name varchar(200),
    salary int
);

INSERT INTO gt_mysql.gt_db1.tb01(name, salary) VALUES ('bob', 14), ('tom', 12);

SELECT * FROM gt_mysql.gt_db1.tb01 ORDER BY name;

UPDATE gt_mysql.gt_db1.tb01 SET name='bob_update' WHERE salary=14;

SELECT * FROM gt_mysql.gt_db1.tb01 ORDER BY name;

DROP TABLE gt_mysql.gt_db1.tb01;

DROP SCHEMA gt_mysql.gt_db1;
