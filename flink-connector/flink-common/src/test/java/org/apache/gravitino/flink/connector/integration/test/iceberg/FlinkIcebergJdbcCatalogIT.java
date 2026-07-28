/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.flink.connector.integration.test.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.flink.connector.iceberg.GravitinoIcebergCatalogFactoryOptions;
import org.apache.gravitino.flink.connector.iceberg.IcebergPropertiesConstants;
import org.apache.gravitino.flink.connector.integration.test.utils.TestUtils;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.MySQLContainer;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.condition.EnabledIf;

/** Iceberg catalog with a JDBC (MySQL) metadata backend. */
@Tag("gravitino-docker-test")
public abstract class FlinkIcebergJdbcCatalogIT extends FlinkIcebergCatalogIT {

  private static final TestDatabaseName TEST_DB_NAME =
      TestDatabaseName.FLINK_ICEBERG_JDBC_CATALOG_IT;
  private static final String NANOSECOND_TIMESTAMP_VALUE = "2024-01-02 03:04:05.123456789";

  private static MySQLContainer mySQLContainer;

  @Test
  @DisabledIf("supportsNanosecondTimestampRoundTrip")
  public void testRejectNanosecondTimestampCreateBeforeMutation() {
    String databaseName = "test_reject_timestamp_nanos_create";

    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          assertNanosecondCreateRejected(catalog, databaseName, "timestamp_ns", "TIMESTAMP(9)");
          assertNanosecondCreateRejected(
              catalog, databaseName, "timestamptz_ns", "TIMESTAMP_LTZ(9)");
        },
        true,
        supportDropCascade());
  }

  @Test
  @DisabledIf("supportsNanosecondTimestampRoundTrip")
  public void testRejectExistingNanosecondTimestampOnLoad() {
    String databaseName = "test_reject_timestamp_nanos_load";
    String tableName = "timestamp_nanos";
    NameIdentifier identifier = NameIdentifier.of(databaseName, tableName);

    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          catalog
              .asTableCatalog()
              .createTable(
                  identifier,
                  new Column[] {
                    Column.of("timestamp_ns", Types.TimestampType.withoutTimeZone(9)),
                    Column.of("timestamptz_ns", Types.TimestampType.withTimeZone(9))
                  },
                  null,
                  ImmutableMap.of(IcebergConstants.FORMAT_VERSION, "3"));

          try {
            org.apache.flink.table.catalog.Catalog flinkCatalog =
                tableEnv.getCatalog(DEFAULT_ICEBERG_CATALOG).orElseThrow(AssertionError::new);
            Exception exception =
                Assertions.assertThrows(
                    Exception.class,
                    () -> flinkCatalog.getTable(new ObjectPath(databaseName, identifier.name())));
            Assertions.assertTrue(
                ExceptionUtils.getStackTrace(exception)
                    .contains("cannot safely round-trip timestamp precision 9"),
                "Expected a nanosecond compatibility rejection, but got: " + exception);
          } finally {
            catalog.asTableCatalog().dropTable(identifier);
          }
        },
        true,
        supportDropCascade());
  }

  @Test
  @EnabledIf("supportsNanosecondTimestampRoundTrip")
  public void testNanosecondTimestampSchemaAndValueRoundTrip() {
    String databaseName = "test_timestamp_nanos_round_trip";
    String tableName = "timestamp_nanos";
    ZoneId originalLocalTimeZone = tableEnv.getConfig().getLocalTimeZone();

    try {
      tableEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
      doWithSchema(
          currentCatalog(),
          databaseName,
          catalog -> {
            TableResult createResult =
                sql(
                    "CREATE TABLE %s ("
                        + "timestamp_ns TIMESTAMP(9), "
                        + "timestamptz_ns TIMESTAMP_LTZ(9)"
                        + ") WITH ("
                        + "'format-version' = '3', "
                        + "'write.format.default' = 'parquet'"
                        + ")",
                    tableName);
            TestUtils.assertTableResult(createResult, ResultKind.SUCCESS);

            NameIdentifier identifier = NameIdentifier.of(databaseName, tableName);
            Table gravitinoTable = catalog.asTableCatalog().loadTable(identifier);
            Assertions.assertEquals(
                Types.TimestampType.withoutTimeZone(9), gravitinoTable.columns()[0].dataType());
            Assertions.assertEquals(
                Types.TimestampType.withTimeZone(9), gravitinoTable.columns()[1].dataType());
            Assertions.assertEquals(
                "3", gravitinoTable.properties().get(IcebergConstants.FORMAT_VERSION));
            Assertions.assertEquals(
                "parquet", gravitinoTable.properties().get("write.format.default"));

            assertFlinkNanosecondSchema(databaseName, tableName);

            TestUtils.assertTableResult(
                sql(
                    "INSERT INTO %s VALUES ("
                        + "CAST('%s' AS TIMESTAMP(9)), "
                        + "CAST('%s' AS TIMESTAMP_LTZ(9))"
                        + ")",
                    tableName, NANOSECOND_TIMESTAMP_VALUE, NANOSECOND_TIMESTAMP_VALUE),
                ResultKind.SUCCESS_WITH_CONTENT,
                Row.of(-1L));

            List<Row> rows = Lists.newArrayList(sql("SELECT * FROM %s", tableName).collect());
            Assertions.assertEquals(1, rows.size());
            Assertions.assertEquals(
                LocalDateTime.parse("2024-01-02T03:04:05.123456789"), rows.get(0).getField(0));
            Assertions.assertEquals(
                Instant.parse("2024-01-02T03:04:05.123456789Z"), rows.get(0).getField(1));
          },
          true,
          supportDropCascade());
    } finally {
      tableEnv.getConfig().setLocalTimeZone(originalLocalTimeZone);
    }
  }

  @Test
  public void testLoadNullableIcebergUnknownAsFlinkNull() {
    String databaseName = "test_load_iceberg_unknown";
    String tableName = "unknown_column";
    NameIdentifier identifier = NameIdentifier.of(databaseName, tableName);

    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          Table created =
              catalog
                  .asTableCatalog()
                  .createTable(
                      identifier,
                      new Column[] {
                        Column.of("id", Types.IntegerType.get()),
                        Column.of("payload", Types.NullType.get())
                      },
                      null,
                      ImmutableMap.of(IcebergConstants.FORMAT_VERSION, "3"));
          Assertions.assertTrue(created.columns()[1].nullable());
          Assertions.assertEquals(Types.NullType.get(), created.columns()[1].dataType());

          try {
            Table loaded = catalog.asTableCatalog().loadTable(identifier);
            Assertions.assertEquals(Types.NullType.get(), loaded.columns()[1].dataType());

            org.apache.flink.table.catalog.Catalog flinkCatalog =
                tableEnv.getCatalog(DEFAULT_ICEBERG_CATALOG).orElseThrow(AssertionError::new);
            CatalogBaseTable flinkTable =
                Assertions.assertDoesNotThrow(
                    () -> flinkCatalog.getTable(new ObjectPath(databaseName, tableName)));
            Schema.UnresolvedPhysicalColumn payloadColumn =
                (Schema.UnresolvedPhysicalColumn)
                    flinkTable.getUnresolvedSchema().getColumns().get(1);
            Assertions.assertEquals(DataTypes.NULL(), payloadColumn.getDataType());
            Assertions.assertTrue(
                ((DataType) payloadColumn.getDataType()).getLogicalType().isNullable());
          } finally {
            catalog.asTableCatalog().dropTable(identifier);
          }
        },
        true,
        supportDropCascade());
  }

  @Override
  protected void initCatalogEnv() throws Exception {
    ContainerSuite containerSuite = ContainerSuite.getInstance();
    containerSuite.startMySQLContainer(TEST_DB_NAME);
    mySQLContainer = containerSuite.getMySQLContainer();
  }

  @Override
  protected Map<String, String> getCatalogConfigs() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_JDBC);
    catalogProperties.put(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI, getUri());
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE, warehouse);
    catalogProperties.put(IcebergConstants.GRAVITINO_JDBC_USER, mySQLContainer.getUsername());
    catalogProperties.put(IcebergConstants.GRAVITINO_JDBC_PASSWORD, mySQLContainer.getPassword());
    catalogProperties.put(IcebergConstants.GRAVITINO_JDBC_DRIVER, getDriverClassName());
    catalogProperties.put(IcebergConstants.IO_IMPL, "org.apache.iceberg.hadoop.HadoopFileIO");
    // Align catalog_name between server-side and Flink-side JdbcCatalog so tables created through
    // Gravitino are visible to the Flink-side reads/writes.
    catalogProperties.put(IcebergConstants.CATALOG_BACKEND_NAME, DEFAULT_ICEBERG_CATALOG);
    // `credential-providers` is intentionally omitted: the lakehouse-iceberg catalog auto-enables
    // `jdbc-user-password` for JDBC backends, so GravitinoIcebergCatalog.open() injects the
    // credentials via credential vending. This exercises the real auto-vending path.
    // `jdbc.user`/`jdbc.password` are only needed for CREATE CATALOG, where Flink opens the
    // catalog before it is persisted in Gravitino and vending is not yet available.
    catalogProperties.put("jdbc.user", mySQLContainer.getUsername());
    catalogProperties.put("jdbc.password", mySQLContainer.getPassword());
    return catalogProperties;
  }

  @Override
  protected String buildCreateCatalogSql(String catalogName) {
    return String.format(
        "create catalog %s with ("
            + "'type'='%s', "
            + "'catalog-backend'='%s',"
            + "'uri'='%s',"
            + "'warehouse'='%s',"
            + "'jdbc-user'='%s',"
            + "'jdbc-password'='%s',"
            + "'jdbc-driver'='%s',"
            + "'jdbc.user'='%s',"
            + "'jdbc.password'='%s'"
            + ")",
        catalogName,
        GravitinoIcebergCatalogFactoryOptions.IDENTIFIER,
        getCatalogBackend(),
        getUri(),
        warehouse,
        mySQLContainer.getUsername(),
        mySQLContainer.getPassword(),
        getDriverClassName(),
        mySQLContainer.getUsername(),
        mySQLContainer.getPassword());
  }

  @Override
  protected String getCatalogBackend() {
    return IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_JDBC;
  }

  @Override
  protected String getUri() {
    return mySQLContainer.getJdbcUrl(TEST_DB_NAME);
  }

  /**
   * Returns whether this Flink and Iceberg runtime pair safely round-trips nanosecond timestamps.
   *
   * @return {@code true} when precision-9 timestamp reads and writes are lossless
   */
  protected boolean supportsNanosecondTimestampRoundTrip() {
    return false;
  }

  private void assertNanosecondCreateRejected(
      org.apache.gravitino.Catalog catalog,
      String databaseName,
      String tableName,
      String flinkType) {
    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () ->
                sql("CREATE TABLE %s (ts %s) WITH ('format-version' = '3')", tableName, flinkType));
    Assertions.assertTrue(
        ExceptionUtils.getStackTrace(exception)
            .contains("cannot safely round-trip timestamp precision 9"),
        "Expected a nanosecond compatibility rejection, but got: " + exception);
    Assertions.assertFalse(
        catalog.asTableCatalog().tableExists(NameIdentifier.of(databaseName, tableName)),
        "A rejected nanosecond table must not be created in Gravitino");
  }

  private void assertFlinkNanosecondSchema(String databaseName, String tableName) {
    org.apache.flink.table.catalog.Catalog flinkCatalog =
        tableEnv.getCatalog(DEFAULT_ICEBERG_CATALOG).orElseThrow(AssertionError::new);
    CatalogBaseTable flinkTable =
        Assertions.assertDoesNotThrow(
            () -> flinkCatalog.getTable(new ObjectPath(databaseName, tableName)));
    List<Schema.UnresolvedColumn> columns = flinkTable.getUnresolvedSchema().getColumns();
    Assertions.assertEquals(
        DataTypes.TIMESTAMP(9), ((Schema.UnresolvedPhysicalColumn) columns.get(0)).getDataType());
    Assertions.assertEquals(
        DataTypes.TIMESTAMP_LTZ(9),
        ((Schema.UnresolvedPhysicalColumn) columns.get(1)).getDataType());
  }

  private String getDriverClassName() {
    try {
      return mySQLContainer.getDriverClassName(TEST_DB_NAME);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to get MySQL driver class name", e);
    }
  }
}
