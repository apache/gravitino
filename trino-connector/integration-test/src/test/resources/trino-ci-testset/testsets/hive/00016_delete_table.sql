-- Test delete with ACID table
-- Table create by integration-test-common/docker-script/init/hive/init.sql
INSERT INTO gt_hive.gt_hive_test_acid.test_delete VALUES (1, 'bob', 12), (2, 'tom', 13);

SELECT * FROM gt_hive.gt_hive_test_acid.test_delete ORDER BY id;

DELETE FROM gt_hive.gt_hive_test_acid.test_delete WHERE id=1;

SELECT * FROM gt_hive.gt_hive_test_acid.test_delete ORDER BY id;

DELETE FROM gt_hive.gt_hive_test_acid.test_delete;

SELECT COUNT(*) FROM gt_hive.gt_hive_test_acid.test_delete;

-- Test delete with partition table
CREATE SCHEMA gt_hive.gt_hive_test_delete;

CREATE TABLE gt_hive.gt_hive_test_delete.tb01 (
    id int,
    name varchar,
    dt varchar
) WITH (partitioned_by = ARRAY['dt']);

INSERT INTO gt_hive.gt_hive_test_delete.tb01 values (1, 'bob', '2025-08-27'), (2, 'tom', '2025-08-27'), (3, 'nancy', '2025-08-28');

SELECT * FROM gt_hive.gt_hive_test_delete.tb01 ORDER BY id;

DELETE FROM gt_hive.gt_hive_test_delete.tb01 where dt='2025-08-27';

SELECT * FROM gt_hive.gt_hive_test_delete.tb01 ORDER BY id;

DELETE FROM gt_hive.gt_hive_test_delete.tb01;

SELECT COUNT(*) FROM gt_hive.gt_hive_test_delete.tb01;

DROP TABLE gt_hive.gt_hive_test_delete.tb01;

DROP SCHEMA gt_hive.gt_hive_test_delete;
