CREATE SCHEMA gt_view_db;
CREATE TABLE gt_view_db.t01 (id integer, name varchar, salary integer);
INSERT INTO gt_view_db.t01 VALUES (1, 'alice', 100), (2, 'bob', 200);

CREATE VIEW gt_view_db.v01 AS SELECT id, name FROM gt_view_db.t01 WHERE salary > 100;
SHOW TABLES FROM gt_view_db;
SELECT * FROM gt_view_db.v01 ORDER BY id;

SHOW CREATE VIEW gt_view_db.v01;

CREATE OR REPLACE VIEW gt_view_db.v01 AS SELECT id, name, salary FROM gt_view_db.t01;
SELECT * FROM gt_view_db.v01 ORDER BY id;

ALTER VIEW gt_view_db.v01 RENAME TO gt_view_db.v02;
SHOW TABLES FROM gt_view_db;
DROP VIEW gt_view_db.v02;
SHOW TABLES FROM gt_view_db;

DROP VIEW gt_view_db.nonexistent_view;

DROP TABLE gt_view_db.t01;
DROP SCHEMA gt_view_db;
