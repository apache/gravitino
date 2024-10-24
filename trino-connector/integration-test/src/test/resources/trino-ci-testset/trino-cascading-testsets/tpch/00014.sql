SELECT 100.00 * sum(CASE
                    WHEN p.type LIKE 'PROMO%'
                      THEN l.extendedprice * (1 - l.discount)
                    ELSE 0
                    END) / sum(l.extendedprice * (1 - l.discount)) AS promo_revenue
FROM
  lineitem AS l,
  part AS p
WHERE
  l.partkey = p.partkey
  AND l.shipdate >= DATE '1995-09-01'
  AND l.shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH
ORDER BY
    promo_revenue DESC
LIMIT 20
;