CREATE SCHEMA gt_iceberg_test_merge;

CREATE TABLE gt_iceberg_test_merge.tb01 (
    name varchar,
    salary int
);

INSERT INTO gt_iceberg_test_merge.tb01(name, salary) VALUES ('bob', 14), ('tom', 12);

SELECT * FROM gt_iceberg_test_merge.tb01 ORDER BY name;

CREATE TABLE gt_iceberg_test_merge.tb02 (
    name varchar,
    salary int
);

INSERT INTO gt_iceberg_test_merge.tb02(name, salary) VALUES ('bob', 15), ('tom', 16), ('nancy', 17);

SELECT * FROM gt_iceberg_test_merge.tb02 ORDER BY name;

MERGE INTO gt_iceberg_test_merge.tb01 t USING gt_iceberg_test_merge.tb02 s
    ON (t.name = s.name)
    WHEN MATCHED AND s.name = 'bob'
        THEN DELETE
    WHEN MATCHED
        THEN UPDATE
            SET salary = s.salary + t.salary
    WHEN NOT MATCHED
        THEN INSERT (name, salary)
              VALUES (s.name, s.salary);

SELECT * FROM gt_iceberg_test_merge.tb01 ORDER BY name;

CREATE TABLE gt_iceberg_test_merge.tb03 (
    name varchar,
    salary int
) WITH (format_version='1');

INSERT INTO gt_iceberg_test_merge.tb03(name, salary) VALUES ('bob', 14), ('tom', 12);

MERGE INTO gt_iceberg_test_merge.tb03 t USING gt_iceberg_test_merge.tb02 s
    ON (t.name = s.name)
    WHEN MATCHED AND s.name = 'bob'
        THEN DELETE
    WHEN MATCHED
        THEN UPDATE
            SET salary = s.salary + t.salary
    WHEN NOT MATCHED
        THEN INSERT (name, salary)
              VALUES (s.name, s.salary);

DROP TABLE gt_iceberg_test_merge.tb01;

DROP TABLE gt_iceberg_test_merge.tb02;

DROP TABLE gt_iceberg_test_merge.tb03;

DROP SCHEMA gt_iceberg_test_merge;
