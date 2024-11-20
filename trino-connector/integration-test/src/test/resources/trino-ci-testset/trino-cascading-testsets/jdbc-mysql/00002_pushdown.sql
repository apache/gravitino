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
