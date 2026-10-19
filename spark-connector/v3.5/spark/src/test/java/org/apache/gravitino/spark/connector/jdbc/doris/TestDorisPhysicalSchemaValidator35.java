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

package org.apache.gravitino.spark.connector.jdbc.doris;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.jdbc.SparkJdbcTypeConverter;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import scala.collection.immutable.HashMap;

/** Unit tests for Doris FE/JDBC physical schema validation. */
public class TestDorisPhysicalSchemaValidator35 {

  @Test
  void testConnectionFailureReportsStageWithoutConnectionDetails() {
    String jdbcUrl = "jdbc:missing-doris-driver://host/db?password=test-only-secret";
    String jdbcPassword = "test-only-secret";
    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DorisPhysicalSchemaValidator35.validate(
                    Identifier.of(new String[] {"db"}, "table"),
                    logicalTable(logicalColumn("id", Types.IntegerType.get(), false)),
                    DataTypes.createStructType(
                        new StructField[] {
                          DataTypes.createStructField("id", DataTypes.IntegerType, false)
                        }),
                    jdbcOptions(
                        jdbcUrl,
                        Map.of(
                            "driver", "com.mysql.cj.jdbc.Driver",
                            "user", "test-user",
                            "password", jdbcPassword)),
                    new SparkJdbcTypeConverter()));

    assertTrue(failure.getMessage().contains("JDBC connection"));
    assertFalse(failure.getMessage().contains(jdbcUrl));
    assertFalse(failure.getMessage().contains(jdbcPassword));
    assertNull(failure.getCause());
  }

  @Test
  void testShowColumnsFailureReportsSafeJdbcDiagnostics() throws Exception {
    String jdbcUrl = "jdbc:doris-validator-test://host/db";
    Driver driver = mock(Driver.class);
    Connection connection = mock(Connection.class);
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    ResultSet jdbcColumns = mock(ResultSet.class);
    Statement statement = mock(Statement.class);
    when(driver.connect(eq(jdbcUrl), any(Properties.class))).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(metadata);
    when(metadata.getColumns("db", "db", "table", "%")).thenReturn(jdbcColumns);
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery(anyString()))
        .thenThrow(new SQLException("test-only-secret", "42000", 1234));

    DorisTestDriver.setDelegate(driver);
    try {
      IllegalArgumentException failure =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  DorisPhysicalSchemaValidator35.validate(
                      Identifier.of(new String[] {"db"}, "table"),
                      logicalTable(logicalColumn("id", Types.IntegerType.get(), false)),
                      DataTypes.createStructType(
                          new StructField[] {
                            DataTypes.createStructField("id", DataTypes.IntegerType, false)
                          }),
                      jdbcOptions(
                          jdbcUrl, Map.of("user", "test-user", "password", "test-only-secret")),
                      new SparkJdbcTypeConverter()));
      assertTrue(failure.getMessage().contains("FE SHOW COLUMNS"));
      assertTrue(failure.getMessage().contains("SQLState 42000"));
      assertTrue(failure.getMessage().contains("vendor code 1234"));
      assertFalse(failure.getMessage().contains("test-only-secret"));
      assertNull(failure.getCause());
    } finally {
      DorisTestDriver.clearDelegate();
    }
  }

  @Test
  void testValidationUsesConfiguredDriverAndConnectionProperties() throws Exception {
    String jdbcUrl = "jdbc:doris-validator-test://host/db";
    Driver driver = mock(Driver.class);
    Connection connection = mock(Connection.class);
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    ResultSet jdbcColumns = mock(ResultSet.class);
    Statement statement = mock(Statement.class);
    ResultSet feColumns = mock(ResultSet.class);
    when(driver.connect(eq(jdbcUrl), any(Properties.class))).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(metadata);
    when(metadata.getColumns("db", "db", "table", "%")).thenReturn(jdbcColumns);
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery("SHOW COLUMNS FROM `db`.`table`")).thenReturn(feColumns);
    Table logicalTable = mock(Table.class);
    when(logicalTable.columns()).thenReturn(new Column[0]);

    DorisTestDriver.setDelegate(driver);
    try {
      DorisPhysicalSchemaValidator35.validate(
          Identifier.of(new String[] {"db"}, "table"),
          logicalTable,
          new StructType(),
          jdbcOptions(
              jdbcUrl,
              Map.of(
                  "user", "vended-user",
                  "password", "test-only-secret",
                  "sslMode", "VERIFY_IDENTITY",
                  "connectTimeout", "5000",
                  "fetchsize", "100")),
          new SparkJdbcTypeConverter());

      ArgumentCaptor<Properties> properties = ArgumentCaptor.forClass(Properties.class);
      verify(driver).connect(eq(jdbcUrl), properties.capture());
      assertEquals("vended-user", properties.getValue().getProperty("user"));
      assertEquals("test-only-secret", properties.getValue().getProperty("password"));
      assertEquals("VERIFY_IDENTITY", properties.getValue().getProperty("sslMode"));
      assertEquals("5000", properties.getValue().getProperty("connectTimeout"));
      assertFalse(properties.getValue().containsKey("fetchsize"));
      assertFalse(properties.getValue().containsKey("dbtable"));
      assertFalse(properties.getValue().containsKey("driver"));
      verify(connection).close();
      verify(jdbcColumns).close();
      verify(feColumns).close();
      verify(statement).close();
    } finally {
      DorisTestDriver.clearDelegate();
    }
  }

  @Test
  void testOrdinaryScalarSchemaPasses() {
    assertDoesNotThrow(
        () ->
            validateSingleColumn(
                Types.IntegerType.get(), DataTypes.IntegerType, "INT", "INT", false));
  }

  @Test
  void testSmallIntMetadataRepresentationsPreserveLogicalShortType() {
    assertEquals(
        DataTypes.ShortType, new SparkJdbcTypeConverter().toSparkType(Types.ShortType.get()));
    for (DataType physicalType : Arrays.asList(DataTypes.ShortType, DataTypes.IntegerType)) {
      for (boolean nullable : new boolean[] {false, true}) {
        assertDoesNotThrow(
            () ->
                validateSingleColumn(
                    Types.ShortType.get(), physicalType, "SMALLINT", "SMALLINT", nullable));
      }
    }
  }

  @Test
  void testSmallIntCompatibilityRequiresMatchingSmallIntMetadata() {
    List<String> typeNames = Arrays.asList("SMALLINT", "TINYINT", "INT", "BIGINT");
    for (String jdbcType : typeNames) {
      for (String feType : typeNames) {
        if ("SMALLINT".equals(jdbcType) && "SMALLINT".equals(feType)) {
          continue;
        }
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validateSingleColumn(
                    Types.ShortType.get(), DataTypes.IntegerType, jdbcType, feType, false));
      }
    }
  }

  @Test
  void testSmallIntCompatibilityDoesNotAllowOtherLogicalOrSparkTypes() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            validateSingleColumn(
                Types.ByteType.get(), DataTypes.IntegerType, "SMALLINT", "SMALLINT", false));
    for (DataType physicalType : Arrays.asList(DataTypes.ByteType, DataTypes.LongType)) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              validateSingleColumn(
                  Types.ShortType.get(), physicalType, "SMALLINT", "SMALLINT", false));
    }
  }

  @Test
  void testDorisDateV2AndTextAreOrdinaryTypes() {
    Identifier identifier = Identifier.of(new String[] {"db"}, "table");
    Column dateColumn = logicalColumn("event_date", Types.DateType.get(), false);
    Column textColumn = logicalColumn("description", Types.StringType.get(), true);
    Table logicalTable = mock(Table.class);
    when(logicalTable.columns()).thenReturn(new Column[] {dateColumn, textColumn});
    StructType physicalSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("event_date", DataTypes.DateType, false),
              DataTypes.createStructField("description", DataTypes.StringType, true)
            });
    List<DorisPhysicalSchemaValidator35.PhysicalColumn> jdbcColumns =
        Arrays.asList(
            new DorisPhysicalSchemaValidator35.PhysicalColumn("event_date", "DATE", false, 0),
            new DorisPhysicalSchemaValidator35.PhysicalColumn("description", "TEXT", true, 1));
    List<DorisPhysicalSchemaValidator35.PhysicalColumn> feColumns =
        Arrays.asList(
            new DorisPhysicalSchemaValidator35.PhysicalColumn("event_date", "DATEV2", false, 0),
            new DorisPhysicalSchemaValidator35.PhysicalColumn("description", "STRING", true, 1));

    assertDoesNotThrow(
        () ->
            DorisPhysicalSchemaValidator35.validateColumns(
                identifier,
                logicalTable,
                physicalSchema,
                jdbcColumns,
                feColumns,
                new SparkJdbcTypeConverter()));
  }

  @Test
  void testDecimalV3AndJdbcDecimalSignaturesMatch() {
    assertDoesNotThrow(
        () ->
            validateSingleColumn(
                Types.DecimalType.of(10, 2),
                DataTypes.createDecimalType(10, 2),
                "DECIMAL(10,2)",
                "DECIMALV3(10,2)",
                false));
  }

  @Test
  void testDefaultDatetimePrecisionMatchesButHigherPrecisionDoesNot() {
    assertDoesNotThrow(
        () ->
            validateSingleColumn(
                Types.TimestampType.withoutTimeZone(0),
                DataTypes.TimestampType,
                "DATETIME",
                "DATETIMEV2(0)",
                true));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            validateSingleColumn(
                Types.TimestampType.withoutTimeZone(0),
                DataTypes.TimestampType,
                "DATETIME",
                "DATETIMEV2(3)",
                true));
  }

  @Test
  void testTypeSignatureDriftFailsClosed() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            validateSingleColumn(
                Types.DecimalType.of(10, 2),
                DataTypes.createDecimalType(10, 2),
                "DECIMAL(10,2)",
                "DECIMAL(20,2)",
                false));
  }

  @Test
  void testFeAndJdbcTypeDriftFailsClosed() {
    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validateSingleColumn(
                    Types.IntegerType.get(), DataTypes.IntegerType, "INT", "BIGINT", false));
    assertTrue(failure.getMessage().contains("FE and JDBC type families differ"));
  }

  @Test
  void testUnsupportedDorisTypeFailsClosed() {
    for (String typeName : Arrays.asList("JSON", "CHAR(4)", "FLOAT")) {
      IllegalArgumentException failure =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  validateSingleColumn(
                      Types.StringType.get(), DataTypes.StringType, typeName, typeName, true));
      assertTrue(failure.getMessage().contains("unsupported Doris type"));
    }
  }

  @Test
  void testUnknownNullabilityFailsClosed() {
    Identifier identifier = Identifier.of(new String[] {"db"}, "table");
    Column logicalColumn = logicalColumn("id", Types.IntegerType.get(), false);
    StructType physicalSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("id", DataTypes.IntegerType, false)});
    List<DorisPhysicalSchemaValidator35.PhysicalColumn> jdbcColumns =
        Arrays.asList(new DorisPhysicalSchemaValidator35.PhysicalColumn("id", "INT", null, 0));
    List<DorisPhysicalSchemaValidator35.PhysicalColumn> feColumns =
        Arrays.asList(new DorisPhysicalSchemaValidator35.PhysicalColumn("id", "INT", false, 0));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisPhysicalSchemaValidator35.validateColumns(
                identifier,
                logicalTable(logicalColumn),
                physicalSchema,
                jdbcColumns,
                feColumns,
                new SparkJdbcTypeConverter()));
  }

  private static void validateSingleColumn(
      Type logicalType, DataType physicalType, String jdbcType, String feType, boolean nullable) {
    DorisPhysicalSchemaValidator35.validateColumns(
        Identifier.of(new String[] {"db"}, "table"),
        logicalTable(logicalColumn("value", logicalType, nullable)),
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("value", physicalType, nullable)}),
        Arrays.asList(
            new DorisPhysicalSchemaValidator35.PhysicalColumn("value", jdbcType, nullable, 0)),
        Arrays.asList(
            new DorisPhysicalSchemaValidator35.PhysicalColumn("value", feType, nullable, 0)),
        new SparkJdbcTypeConverter());
  }

  private static Column logicalColumn(String name, Type type, boolean nullable) {
    Column column = mock(Column.class);
    when(column.name()).thenReturn(name);
    when(column.dataType()).thenReturn(type);
    when(column.nullable()).thenReturn(nullable);
    return column;
  }

  private static Table logicalTable(Column column) {
    Table table = mock(Table.class);
    when(table.columns()).thenReturn(new Column[] {column});
    return table;
  }

  private static JDBCOptions jdbcOptions(String url, Map<String, String> properties) {
    scala.collection.immutable.Map<String, String> all =
        new HashMap<String, String>().updated("driver", DorisTestDriver.class.getName());
    for (Map.Entry<String, String> property : properties.entrySet()) {
      all = all.updated(property.getKey(), property.getValue());
    }
    return new JDBCOptions(url, "db.table", all);
  }
}
