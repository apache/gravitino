call gravitino.system.create_catalog(
    'gt_iceberg_postgresql1_1',
    'lakehouse-iceberg',
    map(
        array['uri', 'catalog-backend', 'warehouse', 'jdbc-user', 'jdbc-password', 'jdbc-database', 'jdbc-driver', 'trino.bypass.join-pushdown.strategy'],
        array['${postgresql_uri}/db', 'jdbc',
            '${hdfs_uri}/user/iceberg/warehouse/TrinoQueryIT', 'postgres', 'postgres', 'db' ,'org.postgresql.Driver', 'EAGER']
    )
);

call gravitino.system.create_catalog(
    'gt_iceberg_postgresql1',
    'lakehouse-iceberg',
    map(
        array['uri', 'catalog-backend', 'warehouse', 'jdbc-user', 'jdbc-password', 'jdbc-database', 'jdbc-driver', 'trino.bypass.join-pushdown.strategy', 'cloud.region-code', 'cloud.trino.connection-url', 'cloud.trino.connection-user', 'cloud.trino.connection-password'],
        array['${postgresql_uri}/db', 'jdbc',
            '${hdfs_uri}/user/iceberg/warehouse/TrinoQueryIT', 'postgres', 'postgres', 'db' ,'org.postgresql.Driver', 'EAGER', 'c2', '${trino_remote_jdbc_uri}', 'admin', '']
    )
);

CREATE SCHEMA gt_iceberg_postgresql1_1.gt_db2;

USE gt_iceberg_postgresql1_1.gt_db2;

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
    f15 TIMESTAMP,
    f16 TIMESTAMP WITH TIME ZONE
);

INSERT INTO tb01 (f1, f3, f4, f5, f6, f7, f10, f11, f12, f13, f14, f15, f16)
VALUES ('Sample text 1', x'65', 123.456, 7.89, 12.34, true, 1000, 1000, 100000, DATE '2024-01-01', TIME '08:00:00',
        TIMESTAMP '2024-01-01 08:00:00', TIMESTAMP '2024-01-01 08:00:00 UTC');

INSERT INTO tb01 (f1, f3, f4, f5, f6, f7, f10, f11, f12, f13, f14, f15, f16)
VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

CREATE TABLE tb02 (
    f1 VARCHAR NOT NULL ,
    f3 VARBINARY NOT NULL ,
    f4 DECIMAL(10, 3) NOT NULL ,
    f5 REAL NOT NULL ,
    f6 DOUBLE NOT NULL ,
    f7 BOOLEAN NOT NULL ,
    f10 INT NOT NULL ,
    f11 INTEGER NOT NULL ,
    f12 BIGINT NOT NULL ,
    f13 DATE NOT NULL ,
    f14 TIME NOT NULL ,
    f15 TIMESTAMP NOT NULL,
    f16 TIMESTAMP WITH TIME ZONE NOT NULL
);

INSERT INTO tb02 (f1, f3, f4, f5, f6, f7, f10, f11, f12, f13, f14, f15, f16)
VALUES ('Sample text 1', x'65', 123.456, 7.89, 12.34, true, 1000, 1000, 100000, DATE '2024-01-01', TIME '08:00:00',
        TIMESTAMP '2024-01-01 08:00:00', TIMESTAMP '2024-01-01 08:00:00 UTC');

CREATE TABLE lineitem(
    orderkey bigint,
    partkey bigint,
    suppkey bigint,
    linenumber integer,
    quantity decimal(12, 2),
    extendedprice decimal(12, 2),
    discount decimal(12, 2),
    tax decimal(12, 2),
    returnflag varchar,
    linestatus varchar,
    shipdate date,
    commitdate date,
    receiptdate date,
    shipinstruct varchar,
    shipmode varchar,
    comment varchar
)
WITH (
    partitioning = ARRAY['year(commitdate)'],
    sorted_by = ARRAY['partkey', 'extendedprice desc']
);

insert into lineitem select * from tpch.tiny.lineitem;

CREATE TABLE tb03(
    orderkey bigint,
    partkey bigint,
    suppkey bigint,
    linenumber integer,
    quantity decimal(12, 2),
    extendedprice decimal(12, 2),
    discount decimal(12, 2),
    tax decimal(12, 2),
    returnflag varchar,
    linestatus varchar,
    shipdate date,
    commitdate date,
    receiptdate date,
    shipinstruct varchar,
    shipmode varchar,
    comment varchar
)
WITH (
    partitioning = ARRAY['day(commitdate)', 'month(shipdate)', 'bucket(partkey, 2)', 'truncate(shipinstruct, 2)'],
    sorted_by = ARRAY['partkey asc nulls last', 'extendedprice DESC NULLS FIRST']
);

USE gt_iceberg_postgresql1.gt_db2;

<RETRY_WITH_NOT_EXISTS> SELECT * FROM gt_iceberg_postgresql1.gt_db2.tb03;
