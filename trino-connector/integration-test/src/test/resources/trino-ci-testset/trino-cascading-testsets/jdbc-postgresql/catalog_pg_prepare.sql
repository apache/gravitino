call gravitino.system.create_catalog(
    'gt_postgresql1',
    'jdbc-postgresql',
     MAP(
        ARRAY['jdbc-url', 'jdbc-user', 'jdbc-password', 'jdbc-database', 'jdbc-driver', 'trino.bypass.join-pushdown.strategy', 'cloud.region-code', 'cloud.trino.connection-url', 'cloud.trino.connection-user', 'cloud.trino.connection-password'],
        ARRAY['${postgresql_uri}/db', 'postgres', 'postgres', 'db', 'org.postgresql.Driver', 'EAGER','c2', '${trino_remote_jdbc_uri}', 'admin', '']
    )
);

call gravitino.system.create_catalog(
    'gt_postgresql1_1',
    'jdbc-postgresql',
    map(
        array['jdbc-url', 'jdbc-user', 'jdbc-password', 'jdbc-database', 'jdbc-driver', 'trino.bypass.join-pushdown.strategy'],
        array['${postgresql_uri}/db', 'postgres', 'postgres', 'db', 'org.postgresql.Driver', 'EAGER']
    )
);

CREATE SCHEMA gt_postgresql1_1.gt_datatype;

CREATE TABLE gt_postgresql1_1.gt_datatype.tb01 (
    name varchar,
    salary int
);

<RETRY_WITH_NOT_EXISTS> SELECT * FROM gt_postgresql1.gt_datatype.tb01;