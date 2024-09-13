CREATE SCHEMA gt_db2;

USE gt_db2;

-- Unsupported Type: CHAR TINYINT, SMALLINT
CREATE TABLE tb01 (
    f1 VARCHAR,
    f3 VARBINARY,
    f4 DECIMAL(10, 3),
    f5 REAL,
    f6 DOUBLE,
    f7 BOOLEAN,
    f10 INT,
    f11 INTEGER,
    f12 BIGINT,
    f13 DATE,
    f14 TIME,
    f15 TIMESTAMP
);

SHOW CREATE TABLE tb01;

INSERT INTO tb01 (f1, f3, f4, f5, f6, f7, f10, f11, f12, f13, f14, f15)
VALUES ('Sample text 1', x'65', 123.456, 7.89, 12.34, true, 1000, 1000, 100000, DATE '2024-01-01', TIME '08:00:00', TIMESTAMP '2024-01-01 08:00:00');

INSERT INTO tb01 (f1, f3, f4, f5, f6, f7, f10, f11, f12, f13, f14, f15)
VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

select * from tb01 order by f1;

CREATE TABLE tb02 (
    f1 VARCHAR not null ,
    f3 VARBINARY not null ,
    f4 DECIMAL(10, 3) not null ,
    f5 REAL not null ,
    f6 DOUBLE not null ,
    f7 BOOLEAN not null ,
    f10 INT not null ,
    f11 INTEGER not null ,
    f12 BIGINT not null ,
    f13 DATE not null ,
    f14 TIME not null ,
    f15 TIMESTAMP not null
);

show create table tb02;

INSERT INTO tb02 (f1, f3, f4, f5, f6, f7, f10, f11, f12, f13, f14, f15)
VALUES ('Sample text 1', x'65', 123.456, 7.89, 12.34, true, 1000, 1000, 100000, DATE '2024-01-01', TIME '08:00:00', TIMESTAMP '2024-01-01 08:00:00');

INSERT INTO tb02 (f1, f3, f4, f5, f6, f7, f10, f11, f12, f13, f14, f15)
VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO tb02 (f1, f3, f4, f5, f6, f10, f11, f12, f13, f14, f15)
VALUES ('Sample text 1', x'65', 123.456, 7.89, 12.34, 1000, 1000, 100000, DATE '2024-01-01', TIME '08:00:00', TIMESTAMP '2024-01-01 08:00:00');

INSERT INTO tb02 (f1, f3, f4, f5, f6, f7, f10, f11, f12, f13, f14, f15)
VALUES ('Sample text 1', x'65', 123.456, 7.89, 12.34, true, 1000, 1000, NULL, DATE '2024-01-01', TIME '08:00:00', TIMESTAMP '2024-01-01 08:00:00');

INSERT INTO tb02 (f1, f3, f4, f5, f6, f7, f10, f11, f12, f13, f14, f15)
VALUES ('Sample text 1', x'65', 123.456, 7.89, 12.34, true, 1000, 1000, 1992382342, DATE '2024-01-01', NULL, TIMESTAMP '2024-01-01 08:00:00');

drop table tb01;

drop table tb02;

drop schema gt_db2;
