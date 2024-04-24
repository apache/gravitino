call gravitino.system.create_catalog(
    'gt_postgresql',
    'jdbc-postgresql',
    map(
        array['jdbc-url', 'jdbc-user', 'jdbc-password', 'jdbc-database', 'jdbc-driver', 'trino.bypass.join-pushdown.strategy'],
        array['${postgresql_uri}/gt_db', 'trino', 'ds123', 'gt_db', 'org.postgresql.Driver', 'EAGER']
    )
);