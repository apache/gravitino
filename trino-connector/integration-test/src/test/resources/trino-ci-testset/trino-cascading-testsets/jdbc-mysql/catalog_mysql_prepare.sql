call gravitino.system.create_catalog(
    'gt_mysql1',
    'jdbc-mysql',
    map(
        array['jdbc-url', 'jdbc-user', 'jdbc-password', 'jdbc-driver', 'trino.bypass.join-pushdown.strategy', 'cloud.region-code', 'cloud.trino.connection-url', 'cloud.trino.connection-user', 'cloud.trino.connection-password'],
        array['${mysql_uri}/?useSSL=false', 'trino', 'ds123', 'com.mysql.cj.jdbc.Driver', 'EAGER','c2', '${trino_remote_jdbc_uri}', 'admin', '']
    )
);

call gravitino.system.create_catalog(
    'gt_mysql1_1',
    'jdbc-mysql',
    map(
        array['jdbc-url', 'jdbc-user', 'jdbc-password', 'jdbc-driver', 'trino.bypass.join-pushdown.strategy'],
        array['${mysql_uri}/?useSSL=false', 'trino', 'ds123', 'com.mysql.cj.jdbc.Driver', 'EAGER']
    )
);

CREATE SCHEMA gt_mysql1_1.gt_db1;

CREATE TABLE gt_mysql1_1.gt_db1.tb03 (id int, name char(20));

<RETRY_WITH_NOT_EXISTS> SELECT * FROM gt_mysql1.gt_db1.tb03;