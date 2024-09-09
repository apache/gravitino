CREATE SCHEMA gt_postgresql.gt_db1;

use gt_postgresql.gt_db1;

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

insert into customer select * from tpch.tiny.customer;

insert into orders select * from tpch.tiny.orders;

-- projection push down, limit push down
explain select custkey from customer limit 10;

-- predicate push down
explain select * from customer where phone like '%2342%' limit 10;

-- aggregating push down
explain select sum(totalprice) from orders;

-- aggregating push down, TopN push down
explain select orderdate, sum(totalprice) from orders group by orderdate order by orderdate limit 10;

-- join push down
explain select customer.custkey, orders.orderkey from customer join orders on customer.custkey = orders.custkey order by orders.orderkey limit 10;

drop table customer;

drop table orders;

drop schema gt_postgresql.gt_db1;;
