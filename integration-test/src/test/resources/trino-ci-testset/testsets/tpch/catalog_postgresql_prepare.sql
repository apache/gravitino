call gravitino.system.create_catalog(
    'gt_postgresql2',
    'jdbc-postgresql',
    map(
        array['jdbc-url', 'jdbc-user', 'jdbc-password', 'jdbc-database', 'jdbc-driver'],
        array['${postgresql_uri}/gt_db', 'trino', 'ds123', 'gt_db', 'org.postgresql.Driver']
    )
);

create schema gt_postgresql2.gt_tpch;
use gt_postgresql2.gt_tpch;

CREATE TABLE customer (
   custkey bigint NOT NULL,
   name varchar(25) NOT NULL,
   address varchar(40) NOT NULL,
   nationkey bigint NOT NULL,
   phone varchar(15) NOT NULL,
   acctbal decimal(12, 2) NOT NULL,
   mktsegment varchar(10) NOT NULL,
   comment varchar(117) NOT NULL
);

CREATE TABLE lineitem (
   orderkey bigint NOT NULL,
   partkey bigint NOT NULL,
   suppkey bigint NOT NULL,
   linenumber integer NOT NULL,
   quantity decimal(12, 2) NOT NULL,
   extendedprice decimal(12, 2) NOT NULL,
   discount decimal(12, 2) NOT NULL,
   tax decimal(12, 2) NOT NULL,
   returnflag varchar(1) NOT NULL,
   linestatus varchar(1) NOT NULL,
   shipdate date NOT NULL,
   commitdate date NOT NULL,
   receiptdate date NOT NULL,
   shipinstruct varchar(25) NOT NULL,
   shipmode varchar(10) NOT NULL,
   comment varchar(44) NOT NULL
);

CREATE TABLE nation (
   nationkey bigint NOT NULL,
   name varchar(25) NOT NULL,
   regionkey bigint NOT NULL,
   comment varchar(152) NOT NULL
);

CREATE TABLE orders (
   orderkey bigint NOT NULL,
   custkey bigint NOT NULL,
   orderstatus varchar(1) NOT NULL,
   totalprice decimal(12, 2) NOT NULL,
   orderdate date NOT NULL,
   orderpriority varchar(15) NOT NULL,
   clerk varchar(15) NOT NULL,
   shippriority integer NOT NULL,
   comment varchar(79) NOT NULL
);

CREATE TABLE part (
   partkey bigint NOT NULL,
   name varchar(55) NOT NULL,
   mfgr varchar(25) NOT NULL,
   brand varchar(10) NOT NULL,
   type varchar(25) NOT NULL,
   size integer NOT NULL,
   container varchar(10) NOT NULL,
   retailprice decimal(12, 2) NOT NULL,
   comment varchar(23) NOT NULL
);

CREATE TABLE partsupp (
   partkey bigint NOT NULL,
   suppkey bigint NOT NULL,
   availqty integer NOT NULL,
   supplycost decimal(12, 2) NOT NULL,
   comment varchar(199) NOT NULL
);

CREATE TABLE region (
   regionkey bigint NOT NULL,
   name varchar(25) NOT NULL,
   comment varchar(152) NOT NULL
);

CREATE TABLE supplier (
   suppkey bigint NOT NULL,
   name varchar(25) NOT NULL,
   address varchar(40) NOT NULL,
   nationkey bigint NOT NULL,
   phone varchar(15) NOT NULL,
   acctbal decimal(12, 2) NOT NULL,
   comment varchar(101) NOT NULL
);

insert into customer select * from tpch.tiny.customer;
insert into lineitem select * from tpch.tiny.lineitem;
insert into nation select * from tpch.tiny.nation;
insert into orders select * from tpch.tiny.orders;
insert into part select * from tpch.tiny.part;
insert into partsupp select * from tpch.tiny.partsupp;
insert into region select * from tpch.tiny.region;
insert into supplier select * from tpch.tiny.supplier;
