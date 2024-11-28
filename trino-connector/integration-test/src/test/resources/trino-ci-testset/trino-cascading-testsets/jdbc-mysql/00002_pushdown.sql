CREATE TABLE gt_mysql1_1.gt_db1.customer (
   custkey bigint NOT NULL,
   name varchar(25) NOT NULL,
   address varchar(40) NOT NULL,
   nationkey bigint NOT NULL,
   phone varchar(15) NOT NULL,
   acctbal decimal(12, 2) NOT NULL,
   mktsegment varchar(10) NOT NULL,
   comment varchar(117) NOT NULL
);

CREATE TABLE gt_mysql1_1.gt_db1.orders (
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

insert into gt_mysql1_1.gt_db1.customer select * from tpch.tiny.customer;

insert into gt_mysql1_1.gt_db1.orders select * from tpch.tiny.orders;

USE gt_mysql1.gt_db1;

SHOW SCHEMAS LIKE 'gt_%1';

SHOW TABLES LIKE 'cus%';

SHOW COLUMNS FROM gt_mysql1.gt_db1.customer;

-- projection push down, limit push down
explain select custkey from customer limit 10;

-- predicate push down
explain select * from customer where phone like '%2342%' limit 10;

-- aggregating push down
explain select sum(totalprice) from orders;

-- aggregating push down, TopN push down
explain select orderdate, sum(totalprice) from orders group by orderdate order by orderdate limit 10;

-- join push down
explain select * from customer join orders on customer.custkey = orders.custkey limit 10;
