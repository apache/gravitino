call gravitino.system.create_catalog(
    'gt_iceberg2',
    'lakehouse-iceberg',
    map(
        array['uri', 'catalog-backend', 'warehouse'],
        array['${hive_uri}', 'hive', '${hdfs_uri}/user/iceberg/warehouse/TrinoQueryIT']
    )
);

create schema gt_iceberg2.gt_tpch2;
use gt_iceberg2.gt_tpch2;

CREATE TABLE customer AS SELECT * FROM tpch.tiny.customer;
CREATE TABLE lineitem AS SELECT * FROM tpch.tiny.lineitem;
CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation;
CREATE TABLE orders AS SELECT * FROM tpch.tiny.orders;
CREATE TABLE part AS SELECT * FROM tpch.tiny.part;
CREATE TABLE partsupp AS SELECT * FROM tpch.tiny.partsupp;
CREATE TABLE region AS SELECT * FROM tpch.tiny.region;
CREATE TABLE supplier AS SELECT * FROM tpch.tiny.supplier;
