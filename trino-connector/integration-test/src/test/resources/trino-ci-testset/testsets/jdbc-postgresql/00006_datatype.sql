CREATE SCHEMA gt_postgresql.gt_db1;

USE gt_postgresql.gt_db1;

-- Unsupported Type: TINYINT
CREATE TABLE tb01 (
    f1 VARCHAR(200),
    f2 CHAR(20),
    f3 VARBINARY,
    f4 DECIMAL(10, 3),
    f5 REAL,
    f6 DOUBLE,
    f7 BOOLEAN,
    f9 SMALLINT,
    f10 INT,
    f11 INTEGER,
    f12 BIGINT,
    f13 DATE,
    f14 TIME,
    f15 TIMESTAMP,
    f16 TIMESTAMP WITH TIME ZONE
);

SHOW CREATE TABLE tb01;

INSERT INTO tb01 (f1, f2, f3, f4, f5, f6, f7, f9, f10, f11, f12, f13, f14, f15, f16)
VALUES ('Sample text 1', 'Text1', x'65', 123.456, 7.89, 12.34, false, 100, 1000, 1000, 100000, DATE '2024-01-01',
        TIME '08:00:00', TIMESTAMP '2024-01-01 08:00:00', TIMESTAMP '2024-01-01 08:00:00 UTC');

INSERT INTO tb01 (f1, f2, f3, f4, f5, f6, f7, f9, f10, f11, f12, f13, f14, f15, f16)
VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

select * from tb01 order by f1;

CREATE TABLE tb02 (
    f1 VARCHAR(200) not null ,
    f2 CHAR(20) not null ,
    f3 VARBINARY not null ,
    f4 DECIMAL(10, 3) not null ,
    f5 REAL not null ,
    f6 DOUBLE not null ,
    f7 BOOLEAN not null ,
    f9 SMALLINT not null ,
    f10 INT not null ,
    f11 INTEGER not null ,
    f12 BIGINT not null ,
    f13 DATE not null ,
    f14 TIME not null ,
    f15 TIMESTAMP not null,
    f16 TIMESTAMP WITH TIME ZONE not null
);

show create table tb02;

INSERT INTO tb02 (f1, f2, f3, f4, f5, f6, f7, f9, f10, f11, f12, f13, f14, f15, f16)
VALUES ('Sample text 1', 'Text1', x'65', 123.456, 7.89, 12.34, false, 100, 1000, 1000, 100000, DATE '2024-01-01',
        TIME '08:00:00', TIMESTAMP '2024-01-01 08:00:00', TIMESTAMP '2024-01-01 08:00:00 UTC');

INSERT INTO tb02 (f1, f2, f3, f4, f5, f6, f7, f9, f10, f11, f12, f13, f14, f15, f16)
VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO tb02 (f1, f2, f3, f4, f5, f6, f7, f9, f10, f11, f12, f13, f14, f15, f16)
VALUES ('Sample text 1', NULL, x'65', 123.456, 7.89, 12.34, false, 100, 1000, 1000, 100000,
        DATE '2024-01-01', TIME '08:00:00', TIMESTAMP '2024-01-01 08:00:00', TIMESTAMP '2024-01-01 08:00:00 UTC');

INSERT INTO tb02 (f1, f2, f3, f4, f5, f6, f7, f9, f10, f11, f12, f13, f14, f15, f16)
VALUES ('Sample text 1', 'same3', x'65', 123.456, 7.89, 12.34, false, 100, 1000, 1000, NULl, DATE '2024-01-01',
        TIME '08:00:00', TIMESTAMP '2024-01-01 08:00:00', TIMESTAMP '2024-01-01 08:00:00 UTC');

INSERT INTO tb02 (f1, f2, f3, f4, f5, f6, f7, f9, f10, f11, f12, f13, f14, f15, f16)
VALUES ('Sample text 1', 'same9', x'65', 123.456, 7.89, 12.34, false, 100, 1000, 1000, 1992382342, DATE '2024-01-01',
        NULL, TIMESTAMP '2024-01-01 08:00:00', TIMESTAMP '2024-01-01 08:00:00 UTC');

CREATE TABLE tb03  (int_array ARRAY(INTEGER));

INSERT INTO tb03 VALUES (ARRAY[1, 2, 3, 4, 5]);

SELECT * FROM tb03;

drop table tb01;

drop table tb02;

drop table tb03;

drop schema gt_postgresql.gt_db1 cascade;
