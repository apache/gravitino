CREATE SCHEMA gt_mysql.gt_db1;

USE gt_mysql.gt_db1;

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
    f14 TIME,
    f15 TIMESTAMP,
    f16 TIMESTAMP WITH TIME ZONE,
    f17 JSON
);

SHOW CREATE TABLE tb01;

INSERT INTO tb01 (f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17)
VALUES ('Sample text 1', 'Text1', x'65', 123.456, 7.89, 12.34, FALSE, 1, 100, 1000, 1000, 100000, DATE '2024-01-01',
        TIME '08:00:00', TIMESTAMP '2024-01-01 08:00:00', TIMESTAMP '2024-01-01 08:00:00 UTC', JSON '{"x" : 300, "y" : "AFRICA", "z" : null}');

INSERT INTO tb01 (f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17)
VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULl, NULl, NULL);

select * from tb01 order by f1;

CREATE TABLE tb02 (
    f1 VARCHAR(200) NOT NULL ,
    f2 CHAR(20) NOT NULL ,
    f3 VARBINARY NOT NULL ,
    f4 DECIMAL(10, 3) NOT NULL ,
    f5 REAL NOT NULL ,
    f6 DOUBLE NOT NULL ,
    f7 BOOLEAN NOT NULL ,
    f8 TINYINT NOT NULL ,
    f9 SMALLINT NOT NULL ,
    f10 INT NOT NULL ,
    f11 INTEGER NOT NULL ,
    f12 BIGINT NOT NULL ,
    f13 DATE NOT NULL ,
    f14 TIME NOT NULL ,
    f15 TIMESTAMP NOT NULL,
    f16 TIMESTAMP WITH TIME ZONE NOT NULL,
    f17 JSON NOT NULL
);

show create table tb02;

INSERT INTO tb02 (f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17)
VALUES ('Sample text 1', 'Text1', x'65', 123.456, 7.89, 12.34, FALSE, 1, 100, 1000, 1000, 100000, DATE '2024-01-01',
        TIME '08:00:00', TIMESTAMP '2024-01-01 08:00:00', TIMESTAMP '2024-01-01 08:00:00 UTC', JSON '{"x" : 300, "y" : "AFRICA", "z" : null}');

INSERT INTO tb02 (f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17)
VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO tb02 (f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17)
VALUES ('Sample text 1', NULL, x'65', 123.456, 7.89, 12.34, FALSE, 1, 100, 1000, 1000, 100000, DATE '2024-01-01',
        TIME '08:00:00', TIMESTAMP '2024-01-01 08:00:00', TIMESTAMP '2024-01-01 08:00:00 UTC', JSON '{"x" : 300, "y" : "AFRICA", "z" : null}');

INSERT INTO tb02 (f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17)
VALUES ('Sample text 1', 'same3', x'65', 123.456, 7.89, 12.34, FALSE, 1, 100, 1000, 1000, NULl, DATE '2024-01-01',
        TIME '08:00:00', TIMESTAMP '2024-01-01 08:00:00', TIMESTAMP '2024-01-01 08:00:00 UTC', JSON '{"x" : 300, "y" : "AFRICA", "z" : null}');

INSERT INTO tb02 (f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17)
VALUES ('Sample text 1', 'same9', x'65', 123.456, 7.89, 12.34, FALSE, 1, 100, 1000, 1000, 1992382342, DATE '2024-01-01',
        NULL, TIMESTAMP '2024-01-01 08:00:00', TIMESTAMP '2024-01-01 08:00:00 UTC', JSON '{}');

drop table tb01;

drop table tb02;

drop schema gt_mysql.gt_db1 cascade;
