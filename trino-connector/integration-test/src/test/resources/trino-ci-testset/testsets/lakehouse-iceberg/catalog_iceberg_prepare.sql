call gravitino.system.create_catalog(
    'gt_iceberg',
    'lakehouse-iceberg',
    map(
        array['uri', 'catalog-backend', 'warehouse'],
        array['${hive_uri}', 'hive', '${hdfs_uri}/user/iceberg/warehouse/TrinoQueryIT']
    )
);

show catalogs;

CREATE SCHEMA gt_iceberg.gt_db2;

USE gt_iceberg.gt_db2;

DROP SCHEMA gt_iceberg.gt_db2;