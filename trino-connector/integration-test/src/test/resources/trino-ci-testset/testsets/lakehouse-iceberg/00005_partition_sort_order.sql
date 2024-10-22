CREATE SCHEMA gt_db2;

USE gt_db2;

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

show create table lineitem;

insert into lineitem select * from tpch.tiny.lineitem;

select * from lineitem order by orderkey, partkey limit 5;

CREATE TABLE tb01(
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

show create table tb01;

drop table tb01;

drop table lineitem;

drop schema gt_db2;
