call gravitino.system.create_catalog(
    'gt_mysql',
    'jdbc-mysql',
    map(
        array['jdbc-url', 'jdbc-user', 'jdbc-password', 'jdbc-driver', 'trino.bypass.join-pushdown.strategy'],
        array['${mysql_uri}/?useSSL=false', 'trino', 'ds123', 'com.mysql.cj.jdbc.Driver', 'EAGER']
    )
);