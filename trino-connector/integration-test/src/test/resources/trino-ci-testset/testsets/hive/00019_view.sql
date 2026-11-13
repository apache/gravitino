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

-- default catalog/schema round trip: create a view via USE with an unqualified source table
-- reference, then switch the session default elsewhere before querying it, so the query can only
-- succeed if the view's own stored default catalog/schema (not the ambient session) is used to
-- resolve "t01".
USE gt_hive.gt_hive_view_db;
CREATE VIEW v03 AS SELECT id, name FROM t01 WHERE salary > 100;
USE gt_hive.information_schema;
SELECT * FROM gt_hive.gt_hive_view_db.v03 ORDER BY id;
DROP VIEW gt_hive.gt_hive_view_db.v03;

-- error cases: drop nonexistent view; create view colliding with existing table name (HMS natural rejection)
DROP VIEW gt_hive.gt_hive_view_db.nonexistent_view;
CREATE VIEW gt_hive.gt_hive_view_db.t01 AS SELECT 1;

-- native Trino view interop: a view created directly through Trino's own native Hive connector
-- (bypassing Gravitino) is encoded using Trino's own native "Presto View" format; Gravitino's
-- Trino connector recognizes this format directly, so the view is visible and queryable through
-- gt_hive too, without going through Gravitino at all to create it.
CREATE VIEW native_hive.gt_hive_view_db.native_v01 AS SELECT id, name FROM native_hive.gt_hive_view_db.t01;
SHOW TABLES FROM gt_hive.gt_hive_view_db;
SELECT * FROM gt_hive.gt_hive_view_db.native_v01 ORDER BY id;
DROP VIEW native_hive.gt_hive_view_db.native_v01;

-- reverse direction: a view created through gt_hive is persisted to Hive Metastore using Trino's
-- own native "Presto View" format, so it is directly visible and queryable through Trino's native
-- Hive connector (native_hive) without going through Gravitino at all, including a timestamp(3)
-- output column.
CREATE TABLE gt_hive.gt_hive_view_db.t02 (id integer, name varchar, created_at timestamp(3));
INSERT INTO gt_hive.gt_hive_view_db.t02 VALUES (1, 'alice', TIMESTAMP '2024-01-01 00:00:00.000'), (2, 'bob', TIMESTAMP '2024-01-02 00:00:00.000');
CREATE VIEW gt_hive.gt_hive_view_db.v04 AS SELECT id, name, created_at FROM gt_hive.gt_hive_view_db.t02 WHERE id = 2;
SHOW CREATE VIEW native_hive.gt_hive_view_db.v04;
SELECT * FROM native_hive.gt_hive_view_db.v04 ORDER BY id;
DROP VIEW gt_hive.gt_hive_view_db.v04;
DROP TABLE gt_hive.gt_hive_view_db.t02;

DROP TABLE gt_hive.gt_hive_view_db.t01;
DROP SCHEMA gt_hive.gt_hive_view_db;
