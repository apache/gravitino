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
package org.apache.gravitino.catalog.generic.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.generic.GenericJdbcMetadataConfig;
import org.apache.gravitino.catalog.generic.converter.GenericJdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.JdbcSchema;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.types.Types;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for generic JDBC schema and table metadata operations. */
public class TestGenericJdbcOperations {

  private DataSource dataSource;
  private GenericJdbcDatabaseOperations databaseOperations;
  private GenericJdbcTableOperations tableOperations;

  @BeforeEach
  public void setUp() throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.put(GenericJdbcMetadataConfig.EXCLUDE_SCHEMAS, "INFORMATION_SCHEMA,HIDDEN");

    JdbcDataSource jdbcDataSource = new JdbcDataSource();
    jdbcDataSource.setUrl("jdbc:h2:mem:generic_jdbc;DB_CLOSE_DELAY=-1");
    jdbcDataSource.setUser("sa");
    dataSource = jdbcDataSource;

    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA APP");
      statement.execute("CREATE SCHEMA HIDDEN");
      statement.execute(
          "CREATE TABLE APP.CUSTOMERS ("
              + "ID INTEGER NOT NULL PRIMARY KEY, "
              + "NAME VARCHAR(40), "
              + "ACTIVE BOOLEAN, "
              + "AMOUNT DECIMAL(10, 2), "
              + "PAYLOAD BINARY(4), "
              + "CREATED_AT TIMESTAMP)");
      statement.execute("CREATE VIEW APP.CUSTOMER_NAMES AS SELECT NAME FROM APP.CUSTOMERS");
    }

    databaseOperations = new GenericJdbcDatabaseOperations();
    databaseOperations.initialize(dataSource, new JdbcExceptionConverter() {}, conf);

    tableOperations = new GenericJdbcTableOperations();
    tableOperations.initialize(
        dataSource,
        new JdbcExceptionConverter() {},
        new GenericJdbcTypeConverter(),
        new JdbcColumnDefaultValueConverter(),
        conf);
    tableOperations.setDatabaseOperation(databaseOperations);
  }

  @AfterEach
  public void tearDown() throws Exception {
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("DROP ALL OBJECTS");
    }
  }

  @Test
  public void testListAndLoadSchemas() {
    List<String> schemas = databaseOperations.listDatabases();

    assertTrue(schemas.contains("APP"));
    assertFalse(schemas.contains("HIDDEN"));
    assertFalse(schemas.contains("INFORMATION_SCHEMA"));

    JdbcSchema schema = databaseOperations.load("APP");
    assertEquals("APP", schema.name());
    assertThrows(NoSuchSchemaException.class, () -> databaseOperations.load("HIDDEN"));
  }

  @Test
  public void testListAndLoadTables() {
    List<String> tables = tableOperations.listTables("APP");

    assertTrue(tables.contains("CUSTOMERS"));
    assertTrue(tables.contains("CUSTOMER_NAMES"));

    JdbcTable table = tableOperations.load("APP", "CUSTOMERS");
    assertEquals("CUSTOMERS", table.name());
    assertEquals(6, table.columns().length);
    assertEquals("ID", table.columns()[0].name());
    assertEquals(Types.IntegerType.get(), table.columns()[0].dataType());
    assertEquals(Types.VarCharType.of(40), table.columns()[1].dataType());
    assertEquals(Types.BooleanType.get(), table.columns()[2].dataType());
    assertEquals(Types.DecimalType.of(10, 2), table.columns()[3].dataType());
    assertEquals(Types.BinaryType.get(), table.columns()[4].dataType());
    assertEquals(Types.TimestampType.withoutTimeZone(), table.columns()[5].dataType());
    assertTrue(
        Arrays.stream(table.index())
            .anyMatch(
                index ->
                    index.type() == Index.IndexType.PRIMARY_KEY
                        && Arrays.deepEquals(new String[][] {{"ID"}}, index.fieldNames())));
  }

  @Test
  public void testReadOnlyOperationsAreRejected() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> databaseOperations.create("NEW_SCHEMA", null, Map.of()));
    assertThrows(UnsupportedOperationException.class, () -> databaseOperations.delete("APP", true));
    assertThrows(
        UnsupportedOperationException.class, () -> tableOperations.drop("APP", "CUSTOMERS"));
    assertThrows(
        UnsupportedOperationException.class,
        () -> tableOperations.rename("APP", "CUSTOMERS", "CUSTOMERS_NEW"));
  }
}
