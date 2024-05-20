call gravitino.system.create_catalog(
    'gt_iceberg2',
    'lakehouse-iceberg',
    map(
        array['uri', 'catalog-backend', 'warehouse'],
        array['${hive_uri}', 'hive', '${hdfs_uri}/user/iceberg/warehouse/TrinoQueryIT']
    )
);

create schema gt_iceberg2.gt_tpch2;
use gt_iceberg2.gt_tpch2;

CREATE TABLE customer (
   custkey bigint,
   name varchar,
   address varchar,
   nationkey bigint,
   phone varchar,
   acctbal decimal(12, 2),
   mktsegment varchar,
   comment varchar
);

CREATE TABLE lineitem (
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
);

CREATE TABLE nation (
   nationkey bigint,
   name varchar,
   regionkey bigint,
   comment varchar
);

CREATE TABLE orders (
   orderkey bigint,
   custkey bigint,
   orderstatus varchar,
   totalprice decimal(12, 2),
   orderdate date,
   orderpriority varchar,
   clerk varchar,
   shippriority integer,
   comment varchar
);

CREATE TABLE part (
   partkey bigint,
   name varchar,
   mfgr varchar,
   brand varchar,
   type varchar,
   size integer,
   container varchar,
   retailprice decimal(12, 2),
   comment varchar
);

CREATE TABLE partsupp (
   partkey bigint,
   suppkey bigint,
   availqty integer,
   supplycost decimal(12, 2),
   comment varchar
);

CREATE TABLE region (
   regionkey bigint,
   name varchar,
   comment varchar
);

CREATE TABLE supplier (
   suppkey bigint,
   name varchar,
   address varchar,
   nationkey bigint,
   phone varchar,
   acctbal decimal(12, 2),
   comment varchar
);

insert into customer select * from tpch.tiny.customer;
insert into lineitem select * from tpch.tiny.lineitem;
insert into nation select * from tpch.tiny.nation;
insert into orders select * from tpch.tiny.orders;
insert into part select * from tpch.tiny.part;
insert into partsupp select * from tpch.tiny.partsupp;
insert into region select * from tpch.tiny.region;
insert into supplier select * from tpch.tiny.supplier;
