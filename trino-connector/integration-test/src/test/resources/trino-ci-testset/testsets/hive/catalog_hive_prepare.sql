call gravitino.system.create_catalog(
    'gt_hive',
    'hive',
    map(
        array['metastore.uris'],
        array['${hive_uri}']
    )
);

-- use hive connector to read external table
create catalog gt_hive_conn using hive with ("hive.metastore.uri" = '${hive_uri}');