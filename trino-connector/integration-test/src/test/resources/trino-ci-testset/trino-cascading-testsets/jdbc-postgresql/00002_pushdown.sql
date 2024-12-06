CREATE TABLE gt_postgresql1_1.gt_push_db1.employee_performance (
   employee_id integer,
   evaluation_date date,
   rating integer
)
COMMENT 'comment';

CREATE TABLE gt_postgresql1_1.gt_push_db1.employees (
  employee_id integer,
  department_id integer,
  job_title varchar(100),
  given_name varchar(100),
  family_name varchar(100),
  birth_date date,
  hire_date date
)
COMMENT 'comment';

INSERT INTO gt_postgresql1_1.gt_push_db1.employee_performance (employee_id, evaluation_date, rating) VALUES
(1, DATE '2018-02-24', 4),
(1, DATE '2016-12-25', 7),
(1, DATE '2023-04-07', 4),
(3, DATE '2012-11-08', 7),
(3, DATE '2019-09-15', 2),
(3, DATE '2017-06-21', 8),
(3, DATE '2019-07-16', 4),
(3, DATE '2015-10-06', 4),
(3, DATE '2021-01-05', 6),
(3, DATE '2014-10-24', 4);

INSERT INTO gt_postgresql1_1.gt_push_db1.employees (employee_id, department_id, job_title, given_name, family_name, birth_date, hire_date) VALUES
(1, 1, 'Manager', 'Gregory', 'Smith', DATE '1968-04-15', DATE '2014-06-04'),
(2, 1, 'Sales Assistant', 'Owen', 'Rivers', DATE '1988-08-13', DATE '2021-02-05'),
(3, 1, 'Programmer', 'Avram', 'Lawrence', DATE '1969-11-21', DATE '2010-09-29'),
(4, 1, 'Sales Assistant', 'Burton', 'Everett', DATE '2001-12-07', DATE '2016-06-25'),
(5, 1, 'Sales Assistant', 'Cedric', 'Barlow', DATE '1972-02-02', DATE '2012-08-15'),
(6, 2, 'Sales Assistant', 'Jasper', 'Mack', DATE '2002-03-29', DATE '2020-09-13'),
(7, 1, 'Sales Assistant', 'Felicia', 'Robinson', DATE '1973-08-21', DATE '2023-05-14'),
(8, 3, 'Sales Assistant', 'Mason', 'Steele', DATE '1964-05-19', DATE '2019-02-06'),
(9, 3, 'Programmer', 'Bernard', 'Cameron', DATE '1995-08-27', DATE '2018-07-12'),
(10, 2, 'Programmer', 'Chelsea', 'Wade', DATE '2007-01-29', DATE '2016-04-16');

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

CREATE TABLE gt_postgresql1_1.gt_push_db1.customer (
   custkey bigint NOT NULL,
   name varchar(25) NOT NULL,
   address varchar(40) NOT NULL,
   nationkey bigint NOT NULL,
   phone varchar(15) NOT NULL,
   acctbal decimal(12, 2) NOT NULL,
   mktsegment varchar(10) NOT NULL,
   comment varchar(117) NOT NULL
);

CREATE TABLE gt_postgresql1_1.gt_push_db1.orders (
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

INSERT INTO gt_postgresql1_1.gt_push_db1.customer SELECT * FROM tpch.tiny.customer;

INSERT INTO gt_postgresql1_1.gt_push_db1.orders SELECT * FROM tpch.tiny.orders;

USE gt_postgresql1.gt_push_db1;

SHOW SCHEMAS LIKE 'gt_push_%1';

SHOW TABLES LIKE 'cus%';

SHOW COLUMNS FROM gt_postgresql1.gt_push_db1.customer;

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