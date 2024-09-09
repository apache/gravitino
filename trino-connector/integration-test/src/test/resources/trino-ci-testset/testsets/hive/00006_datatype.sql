CREATE SCHEMA gt_hive.gt_db1;

USE gt_hive.gt_db1;

-- Unsupported Type: TIME
CREATE TABLE tb01 (
    f1 VARCHAR(200),
    f2 CHAR(20),
    f3 VARBINARY,
    f4 DECIMAL(10, 3),
    f5 REAL,
    f6 DOUBLE,
    f7 BOOLEAN,
    f8 TINYINT,
    f9 SMALLINT,
    f10 INT,
    f11 INTEGER,
    f12 BIGINT,
    f13 DATE,
    f15 TIMESTAMP
);

SHOW CREATE TABLE tb01;

INSERT INTO tb01 (f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f15)
VALUES ('Sample text 1', 'Text1', x'65', 123.456, 7.89, 12.34, false, 1, 100, 1000, 1000, 100000, DATE '2024-01-01', TIMESTAMP '2024-01-01 08:00:00');

INSERT INTO tb01 (f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f15)
VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

select * from tb01 order by f1;

drop table tb01;

drop schema gt_hive.gt_db1 cascade;
