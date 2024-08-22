call gravitino.system.create_catalog(
    'gt_hive',
    'hive',
    map(
        array['metastore.uris'],
        array['${hive_uri}']
    )
);