# Spark Iceberg + Lance Demo

This directory contains a local demo flow for running Apache Spark against both Gravitino-backed Iceberg REST and Lance REST services at the same time.

## Files

- `bootstrap_gravitino_lakehouse.sh`: rewrites the demo `gravitino.conf`, restarts Gravitino, creates `metalake=test`, and creates the `iceberg` and `lance_catalog` catalogs.
- `start_spark_iceberg_lance.sh`: starts `spark-sql` with the Gravitino Spark connector, Iceberg REST catalog, and Lance REST catalog. Lance storage config comes from the Gravitino catalog, not Spark conf.
- `test_iceberg_lance.sql`: validation SQL for namespace, table, insert, and select checks on both catalogs.

## Usage

```bash
scripts/demo/bootstrap_gravitino_lakehouse.sh
scripts/demo/start_spark_iceberg_lance.sh -f scripts/demo/test_iceberg_lance.sql
```

If the packaged Spark connector jar is missing or incompatible, rebuild the local Spark 3.5 runtime jar first:

```bash
GRAVITINO_FORCE_LOCAL_CONNECTOR_BUILD=true \
  scripts/demo/start_spark_iceberg_lance.sh -f scripts/demo/test_iceberg_lance.sql
```

## Lance Notes

- The demo Lance catalog stores its MinIO/S3 settings in Gravitino, so Spark only needs the catalog URI and parent catalog name.
- The default demo SQL is kept compatible with older Lance bundles by using plain `OPTIMIZE` and `VACUUM` without `WITH (...)` options.
- With `org.lance:lance-spark-bundle-3.5_2.12:0.2.0`, basic Lance REST table create and insert work, and `CREATE INDEX` works.
- On the current Spark 3.5.3 stack, `WITH (...)` options for Lance extension commands fail with a `NamedArgument` binary-compatibility error.
- On the current Spark 3.5.3 plus Lance 0.2.0 stack, `SHOW INDEXES` still fails during Spark analysis after index creation.
- Re-running Lance tests against the same fixed S3 location may require manual cleanup of the corresponding MinIO prefix, because dropping the table metadata does not always remove the underlying dataset files.
