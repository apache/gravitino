-- Table create by integration-test-common/docker-script/init/hive/init.sql
INSERT INTO gt_hive.gt_hive_test_acid.test_merge VALUES (1, 'bob', 12), (2, 'tom', 13);

SELECT * FROM gt_hive.gt_hive_test_acid.test_merge ORDER BY id;

CREATE SCHEMA gt_hive.gt_hive_test_merge;

CREATE TABLE gt_hive.gt_hive_test_merge.tb01 (
    name varchar,
    salary int
);

INSERT INTO gt_hive.gt_hive_test_merge.tb01(name, salary) VALUES ('bob', 14), ('tom', 12), ('nancy', 17);

SELECT * FROM gt_hive.gt_hive_test_merge.tb01 ORDER BY name;

MERGE INTO gt_hive.gt_hive_test_acid.test_merge t USING gt_hive.gt_hive_test_merge.tb01 s
    ON (t.name = s.name)
    WHEN MATCHED AND s.name = 'bob'
        THEN DELETE
    WHEN MATCHED
        THEN UPDATE
            SET salary = s.salary + t.salary
    WHEN NOT MATCHED
        THEN INSERT (id, name, salary)
              VALUES (3, s.name, s.salary);

SELECT * FROM gt_hive.gt_hive_test_acid.test_merge ORDER BY name;

DROP TABLE gt_hive.gt_hive_test_merge.tb01

DROP SCHEMA gt_hive.gt_hive_test_merge
