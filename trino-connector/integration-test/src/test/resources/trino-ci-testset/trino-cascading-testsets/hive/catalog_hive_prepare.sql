call gravitino.system.create_catalog(
    'gt_hive1',
    'hive',
     MAP(
        ARRAY['metastore.uris', 'cloud.region-code', 'cloud.trino.connection-url', 'cloud.trino.connection-user', 'cloud.trino.connection-password'],
        ARRAY['${hive_uri}', 'c2', '${trino_remote_jdbc_uri}', 'admin', '']
    )
);

call gravitino.system.create_catalog(
    'gt_hive1_1',
    'hive',
    map(
        array['metastore.uris'],
        array['${hive_uri}']
    )
);

CREATE SCHEMA gt_hive1_1.gt_datatype;
CREATE TABLE gt_hive1_1.gt_datatype.tb03 (name char);
INSERT INTO gt_hive1_1.gt_datatype.tb03 VALUES ('a');

<RETRY_WITH_NOT_EXISTS> SELECT * FROM gt_hive1.gt_datatype.tb03;