-- Iceberg checks
SHOW NAMESPACES IN iceberg_rest;

CREATE NAMESPACE IF NOT EXISTS iceberg_rest.demo;

SHOW TABLES IN iceberg_rest.demo;

DROP TABLE IF EXISTS iceberg_rest.demo.orders_iceberg;

CREATE TABLE iceberg_rest.demo.orders_iceberg (
  id INT,
  item STRING,
  amount DOUBLE
)
USING iceberg;

INSERT INTO iceberg_rest.demo.orders_iceberg
VALUES
  (1, 'book', 12.5),
  (2, 'pen', 3.25),
  (3, 'notebook', 7.75);

SELECT * FROM iceberg_rest.demo.orders_iceberg ORDER BY id;

-- Iceberg extension checks
DROP TABLE IF EXISTS iceberg_rest.demo.orders_iceberg_ext;

CREATE TABLE iceberg_rest.demo.orders_iceberg_ext (
  id INT,
  item STRING,
  amount DOUBLE
)
USING iceberg;

INSERT INTO iceberg_rest.demo.orders_iceberg_ext
VALUES
  (1, 'book', 12.5),
  (2, 'pen', 3.25),
  (3, 'notebook', 7.75);

DELETE FROM iceberg_rest.demo.orders_iceberg_ext
WHERE id = 2;

UPDATE iceberg_rest.demo.orders_iceberg_ext
SET amount = amount + 1.0
WHERE id = 1;

MERGE INTO iceberg_rest.demo.orders_iceberg_ext t
USING (
  SELECT 3 AS id, 'notebook-plus' AS item, 9.99 AS amount
  UNION ALL
  SELECT 4 AS id, 'marker' AS item, 5.50 AS amount
) s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET item = s.item, amount = s.amount
WHEN NOT MATCHED THEN INSERT *;

SELECT * FROM iceberg_rest.demo.orders_iceberg_ext ORDER BY id;

-- Iceberg schema and maintenance checks
DROP TABLE IF EXISTS iceberg_rest.demo.orders_iceberg_more_ext;

CREATE TABLE iceberg_rest.demo.orders_iceberg_more_ext (
  id INT,
  item STRING,
  amount DOUBLE
)
USING iceberg;

INSERT INTO iceberg_rest.demo.orders_iceberg_more_ext
VALUES
  (1, 'book', 12.5),
  (2, 'pen', 3.25);

ALTER TABLE iceberg_rest.demo.orders_iceberg_more_ext
ADD COLUMN category STRING;

UPDATE iceberg_rest.demo.orders_iceberg_more_ext
SET category = 'office'
WHERE id = 2;

ALTER TABLE iceberg_rest.demo.orders_iceberg_more_ext
RENAME COLUMN item TO product_name;

SELECT * FROM iceberg_rest.demo.orders_iceberg_more_ext ORDER BY id;

CALL iceberg_rest.system.expire_snapshots('demo.orders_iceberg_more_ext');

-- Lance checks
SHOW NAMESPACES IN lance;

CREATE NAMESPACE IF NOT EXISTS lance.demo;

SHOW TABLES IN lance.demo;

DROP TABLE IF EXISTS lance.demo.orders_lance;

CREATE TABLE lance.demo.orders_lance (
  id INT,
  item STRING,
  amount DOUBLE
)
USING lance
LOCATION 's3://contacts/raw/lance/demo/orders_lance.lance/';

INSERT INTO lance.demo.orders_lance
VALUES
  (1, 'book', 12.5),
  (2, 'pen', 3.25),
  (3, 'notebook', 7.75);

INSERT INTO lance.demo.orders_lance
VALUES
  (4, 'pencil', 1.5),
  (5, 'eraser', 0.99);

OPTIMIZE lance.demo.orders_lance;

VACUUM lance.demo.orders_lance;

SELECT * FROM lance.demo.orders_lance ORDER BY id;

-- Lance vector checks
DROP TABLE IF EXISTS lance.demo.orders_lance_vectors;

CREATE TABLE lance.demo.orders_lance_vectors (
  id INT,
  embedding ARRAY<FLOAT> NOT NULL,
  segment STRING
)
USING lance
LOCATION 's3://contacts/raw/lance/demo/orders_lance_vectors.lance/'
TBLPROPERTIES (
  'embedding.arrow.fixed-size-list.size' = '4'
);

INSERT INTO lance.demo.orders_lance_vectors
VALUES
  (1, array(CAST(0.10 AS FLOAT), CAST(0.20 AS FLOAT), CAST(0.30 AS FLOAT), CAST(0.40 AS FLOAT)), 'office'),
  (2, array(CAST(0.90 AS FLOAT), CAST(0.10 AS FLOAT), CAST(0.20 AS FLOAT), CAST(0.00 AS FLOAT)), 'office'),
  (3, array(CAST(0.30 AS FLOAT), CAST(0.60 AS FLOAT), CAST(0.10 AS FLOAT), CAST(0.20 AS FLOAT)), 'study');

ALTER TABLE lance.demo.orders_lance_vectors
CREATE INDEX idx_orders_lance_vectors_id USING btree (id);

SELECT id, size(embedding) AS embedding_dim, segment
FROM lance.demo.orders_lance_vectors
ORDER BY id;

-- Iceberg + Lance fusion query
WITH query_vector AS (
  SELECT array(
    CAST(0.20 AS FLOAT),
    CAST(0.30 AS FLOAT),
    CAST(0.10 AS FLOAT),
    CAST(0.40 AS FLOAT)
  ) AS target_embedding
),
lance_scores AS (
  SELECT
    v.id,
    v.segment,
    aggregate(
      zip_with(v.embedding, q.target_embedding, (x, y) -> CAST(x * y AS DOUBLE)),
      CAST(0.0 AS DOUBLE),
      (acc, x) -> acc + x
    ) AS similarity_score
  FROM lance.demo.orders_lance_vectors v
  CROSS JOIN query_vector q
)
SELECT
  i.id,
  i.item,
  i.amount,
  s.segment,
  ROUND(s.similarity_score, 6) AS similarity_score
FROM iceberg_rest.demo.orders_iceberg i
JOIN lance_scores s
  ON i.id = s.id
ORDER BY similarity_score DESC, i.id;
