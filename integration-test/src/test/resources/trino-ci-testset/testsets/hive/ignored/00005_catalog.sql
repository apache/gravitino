call gravitino.system.create_catalog(
    'gt_hive_test_xxx1',
    'hive',
    map(
        array['metastore.uris', 'hive.immutable-partitions', 'hive.target-max-file-size', 'hive.create-empty-bucket-files', 'hive.validate-bucketing'],
        array['${hive_uri}', 'true', '1GB', 'true', 'true']
    )
);

show catalogs like 'test.gt_hive_test_xxx1';

CALL gravitino.system.drop_catalog('gt_hive');