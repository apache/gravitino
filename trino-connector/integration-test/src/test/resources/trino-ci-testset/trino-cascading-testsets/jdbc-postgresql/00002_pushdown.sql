USE gt_postgresql1.gt_push_db1;

SELECT
  given_name,
  family_name,
  rating
FROM gt_postgresql1.gt_push_db1.employee_performance AS p
JOIN gt_postgresql1.gt_push_db1.employees AS e
  ON p.employee_id = e.employee_id
ORDER BY
rating DESC, given_name
LIMIT 10;

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