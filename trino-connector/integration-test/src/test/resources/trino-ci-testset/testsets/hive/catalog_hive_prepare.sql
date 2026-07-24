call gravitino.system.create_catalog(
    'gt_hive',
    'hive',
    map(
        array['metastore.uris'],
        array['${hive_uri}']
    )
);

CREATE CATALOG native_hive USING hive
WITH (
    "hive.metastore.uri" = '${hive_uri}'
);