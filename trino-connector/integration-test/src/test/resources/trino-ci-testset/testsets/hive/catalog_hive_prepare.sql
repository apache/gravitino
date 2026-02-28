call gravitino.system.create_catalog(
    'gt_hive',
    'hive',
    map(
        array['metastore.uris', 'trino.bypass.fs.hadoop.enabled'],
        array['${hive_uri}', 'true']
    )
);