call gravitino.system.create_catalog(
    'gt_mysql_xxx1',
    'jdbc-mysql',
    map(
        array['jdbc-url', 'jdbc-user', 'jdbc-password', 'jdbc-driver'],
        array['${mysql_uri}/?useSSL=false', 'trino', 'ds123', 'com.mysql.cj.jdbc.Driver']
    )
);

select * from gravitino.system.catalog where name = 'gt_mysql_xxx1';

call gravitino.system.alter_catalog(
    'gt_mysql_xxx1',
    map(
        array['trino.bypass.join-pushdown.strategy', 'test_key'],
        array['EAGER', 'test_value']
    )
);

select * from gravitino.system.catalog where name = 'gt_mysql_xxx1';

call gravitino.system.alter_catalog(
    'gt_mysql_xxx1',
    map(),
    array['trino.bypass.join-pushdown.strategy']
);

select * from gravitino.system.catalog where name = 'gt_mysql_xxx1';

call gravitino.system.alter_catalog(
    catalog => 'gt_mysql_xxx1',
    set_properties => map(
        array['trino.bypass.join-pushdown.strategy'],
        array['EAGER']
    ),
    remove_properties => array['test_key']
);

select * from gravitino.system.catalog where name = 'gt_mysql_xxx1';

call gravitino.system.drop_catalog('gt_mysql_xxx1');