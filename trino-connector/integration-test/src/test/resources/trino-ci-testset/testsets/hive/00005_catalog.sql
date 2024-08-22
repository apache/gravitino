call gravitino.system.create_catalog(
    'gt_hive_xxx1',
    'hive',
     map(
        array['metastore.uris'],
        array['${hive_uri}']
     )
);

show catalogs like 'gt_hive_xxx1';

CALL gravitino.system.drop_catalog('gt_hive_xxx1');

show catalogs like 'gt_hive_xxx1';

call gravitino.system.create_catalog(
    'gt_hive_xxx1',
    'hive',
     map(
        array['metastore.uris'],
        array['${hive_uri}']
     )
);

show catalogs like 'gt_hive_xxx1';

CALL gravitino.system.drop_catalog('gt_hive_xxx1');
