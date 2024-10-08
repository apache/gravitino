select count(*) from customer;
select count(*) from lineitem;
select count(*) from nation;
select count(*) from orders;
select count(*) from part;
select count(*) from partsupp;
select count(*) from region;
select count(*) from supplier;

SELECT * FROM customer ORDER BY custkey, name, address LIMIT 10;
SELECT * FROM lineitem ORDER BY orderkey, partkey, suppkey LIMIT 10;
SELECT * FROM nation ORDER BY nationkey, name, regionkey LIMIT 10;
SELECT * FROM orders ORDER BY orderkey, custkey, orderstatus LIMIT 10;
SELECT * FROM part ORDER BY partkey, name, mfgr LIMIT 10;
SELECT * FROM partsupp ORDER BY partkey, suppkey, availqty LIMIT 10;
SELECT * FROM region ORDER BY regionkey, name, comment LIMIT 10;
SELECT * FROM supplier ORDER BY suppkey, name, address LIMIT 10;