call gravitino.system.create_catalog(
    'gt_iceberg_mysql',
    'lakehouse-iceberg',
    map(
        array['uri', 'catalog-backend', 'warehouse', 'jdbc-user', 'jdbc-password', 'jdbc-driver'],
        array['${mysql_uri}/iceberg_db?createDatabaseIfNotExist=true&useSSL=false', 'jdbc',
            '${hdfs_uri}/user/iceberg/warehouse/TrinoQueryIT', 'trino', 'ds123', 'com.mysql.cj.jdbc.Driver']
    )
);

show catalogs;

CREATE SCHEMA gt_iceberg_mysql.gt_db2;

USE gt_iceberg_mysql.gt_db2;

DROP SCHEMA gt_iceberg_mysql.gt_db2;
