call gravitino.system.create_catalog(
    'gt_hive2',
    'hive',
    map(
        array['metastore.uris'],
        array['${hive_uri}']
    )
);

create schema gt_hive2.gt_tpch;
use gt_hive2.gt_tpch;

CREATE TABLE customer (
   custkey bigint,
   name varchar(25),
   address varchar(40),
   nationkey bigint,
   phone varchar(15),
   acctbal decimal(12, 2),
   mktsegment varchar(10),
   comment varchar(117)
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
   returnflag varchar(1),
   linestatus varchar(1),
   shipdate date,
   commitdate date,
   receiptdate date,
   shipinstruct varchar(25),
   shipmode varchar(10),
   comment varchar(44)
);

CREATE TABLE nation (
   nationkey bigint,
   name varchar(25),
   regionkey bigint,
   comment varchar(152)
);

CREATE TABLE orders (
   orderkey bigint,
   custkey bigint,
   orderstatus varchar(1),
   totalprice decimal(12, 2),
   orderdate date,
   orderpriority varchar(15),
   clerk varchar(15),
   shippriority integer,
   comment varchar(79)
);

CREATE TABLE part (
   partkey bigint,
   name varchar(55),
   mfgr varchar(25),
   brand varchar(10),
   type varchar(25),
   size integer,
   container varchar(10),
   retailprice decimal(12, 2),
   comment varchar(23)
);

CREATE TABLE partsupp (
   partkey bigint,
   suppkey bigint,
   availqty integer,
   supplycost decimal(12, 2),
   comment varchar(199)
);

CREATE TABLE region (
   regionkey bigint,
   name varchar(25),
   comment varchar(152)
);

CREATE TABLE supplier (
   suppkey bigint,
   name varchar(25),
   address varchar(40),
   nationkey bigint,
   phone varchar(15),
   acctbal decimal(12, 2),
   comment varchar(101)
);

insert into customer select * from tpch.tiny.customer;
insert into lineitem select * from tpch.tiny.lineitem;
insert into nation select * from tpch.tiny.nation;
insert into orders select * from tpch.tiny.orders;
insert into part select * from tpch.tiny.part;
insert into partsupp select * from tpch.tiny.partsupp;
insert into region select * from tpch.tiny.region;
insert into supplier select * from tpch.tiny.supplier;
