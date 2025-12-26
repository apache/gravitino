-- Table create by integration-test-common/docker-script/init/hive/init.sql
INSERT INTO gt_hive.gt_hive_test_acid.test_update VALUES (1, 'bob', 12), (2, 'tom', 13);

SELECT * FROM gt_hive.gt_hive_test_acid.test_update ORDER BY id;

UPDATE gt_hive.gt_hive_test_acid.test_update SET name='bob_update' WHERE id=1;

SELECT * FROM gt_hive.gt_hive_test_acid.test_update ORDER BY id;
