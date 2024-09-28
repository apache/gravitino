SELECT
  c.name,
  c.custkey,
  o.orderkey,
  o.orderdate,
  o.totalprice,
  sum(l.quantity)
FROM
  customer AS c,
  orders AS o,
  lineitem AS l
WHERE
  o.orderkey IN (
    SELECT l.orderkey
    FROM
      lineitem AS l
    GROUP BY
      l.orderkey
    HAVING
      sum(l.quantity) > 300
  )
  AND c.custkey = o.custkey
  AND o.orderkey = l.orderkey
GROUP BY
  c.name,
  c.custkey,
  o.orderkey,
  o.orderdate,
  o.totalprice
ORDER BY
  o.totalprice DESC,
  o.orderdate
LIMIT 20
;