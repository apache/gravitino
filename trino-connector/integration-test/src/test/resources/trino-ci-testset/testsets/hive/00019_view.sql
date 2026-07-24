CREATE SCHEMA gt_hive.gt_hive_view_db;
CREATE TABLE gt_hive.gt_hive_view_db.t01 (id integer, name varchar, salary integer);
INSERT INTO gt_hive.gt_hive_view_db.t01 VALUES (1, 'alice', 100), (2, 'bob', 200);

-- create + query + show tables
CREATE VIEW gt_hive.gt_hive_view_db.v01 AS SELECT id, name FROM gt_hive.gt_hive_view_db.t01 WHERE salary > 100;
SHOW TABLES FROM gt_hive.gt_hive_view_db;
SELECT * FROM gt_hive.gt_hive_view_db.v01 ORDER BY id;

-- show create view
SHOW CREATE VIEW gt_hive.gt_hive_view_db.v01;

-- create or replace
CREATE OR REPLACE VIEW gt_hive.gt_hive_view_db.v01 AS SELECT id, name, salary FROM gt_hive.gt_hive_view_db.t01;
SELECT * FROM gt_hive.gt_hive_view_db.v01 ORDER BY id;

-- rename + drop
ALTER VIEW gt_hive.gt_hive_view_db.v01 RENAME TO gt_hive.gt_hive_view_db.v02;
SHOW TABLES FROM gt_hive.gt_hive_view_db;
DROP VIEW gt_hive.gt_hive_view_db.v02;
SHOW TABLES FROM gt_hive.gt_hive_view_db;

-- error cases: drop nonexistent view; create view colliding with existing table name (HMS natural rejection)
DROP VIEW gt_hive.gt_hive_view_db.nonexistent_view;
CREATE VIEW gt_hive.gt_hive_view_db.t01 AS SELECT 1;

-- native Trino view interop: a view created directly through Trino's own Hive connector
-- (bypassing Gravitino) encodes its body in Trino's internal format, not plain SQL, so it must
-- not be exposed as a readable Trino view through Gravitino.
CREATE VIEW native_hive.gt_hive_view_db.native_v01 AS SELECT id, name FROM native_hive.gt_hive_view_db.t01;
SHOW TABLES FROM gt_hive.gt_hive_view_db;
SELECT * FROM gt_hive.gt_hive_view_db.native_v01 ORDER BY id;
DROP VIEW native_hive.gt_hive_view_db.native_v01;

DROP TABLE gt_hive.gt_hive_view_db.t01;
DROP SCHEMA gt_hive.gt_hive_view_db;
