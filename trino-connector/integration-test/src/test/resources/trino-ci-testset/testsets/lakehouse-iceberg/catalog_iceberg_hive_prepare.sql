call gravitino.system.create_catalog(
    'gt_iceberg',
    'lakehouse-iceberg',
    map(
        array['uri', 'catalog-backend', 'warehouse', 'trino.bypass.fs.hadoop.enabled'],
        array['${hive_uri}', 'hive', '${hdfs_uri}/user/iceberg/warehouse/TrinoQueryIT', 'true']
    )
);

show catalogs;

CREATE SCHEMA gt_iceberg.gt_db2;

USE gt_iceberg.gt_db2;

DROP SCHEMA gt_iceberg.gt_db2;