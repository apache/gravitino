call gravitino.system.create_catalog(
    'gt_mysql_xxx1',
    'jdbc-mysql',
    map(
        array['jdbc-url', 'jdbc-user', 'jdbc-password', 'jdbc-driver'],
        array['${mysql_uri}/?useSSL=false', 'trino', 'ds123', 'com.mysql.cj.jdbc.Driver']
    )
);

show catalogs like 'gt_mysql_xxx1';

call gravitino.system.create_catalog(
    'gt_mysql_xxx1',
    'jdbc-mysql',
    map(
        array['jdbc-url', 'jdbc-user', 'jdbc-password', 'jdbc-driver'],
        array['${mysql_uri}/?useSSL=false', 'trino', 'ds123', 'com.mysql.cj.jdbc.Driver']
    )
);

call gravitino.system.create_catalog(
    catalog => 'gt_mysql_xxx1',
    provider => 'jdbc-mysql',
    properties => map(
        array['jdbc-url', 'jdbc-user', 'jdbc-password', 'jdbc-driver'],
        array['${mysql_uri}/?useSSL=false', 'trino', 'ds123', 'com.mysql.cj.jdbc.Driver']
    ),
    ignore_exist => true
);

CALL gravitino.system.drop_catalog('gt_mysql_xxx1');

show catalogs like 'gt_mysql_xxx1';

CALL gravitino.system.drop_catalog('gt_mysql_xxx1');

CALL gravitino.system.drop_catalog('gt_mysql_xxx1', true);

call gravitino.system.create_catalog(
    'gt_mysql_xxx1',
    'jdbc-mysql',
    map(
        array['jdbc-url', 'jdbc-user', 'jdbc-password', 'jdbc-driver'],
        array['${mysql_uri}/?useSSL=false', 'trino', 'ds123', 'com.mysql.cj.jdbc.Driver']
    )
);

show catalogs like 'gt_mysql_xxx1';

CALL gravitino.system.drop_catalog(
    catalog => 'gt_mysql_xxx1', ignore_not_exist => true);

show catalogs like 'gt_mysql_xxx1';
