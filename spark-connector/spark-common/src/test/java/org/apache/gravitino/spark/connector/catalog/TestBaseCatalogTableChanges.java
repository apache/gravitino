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
package org.apache.gravitino.spark.connector.catalog;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTableChangeConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.jdbc.JdbcPropertiesConverter;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.connector.catalog.ColumnDefaultValue;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/** Table create and alter requests that Spark sends to a Gravitino catalog. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestBaseCatalogTableChanges {

  private static final Identifier IDENT = Identifier.of(new String[] {"schema"}, "t");
  private static final NameIdentifier GRAVITINO_IDENT = NameIdentifier.of("schema", "t");

  private TableCatalog gravitinoTableCatalog;
  private org.apache.spark.sql.connector.catalog.TableCatalog sparkCatalog;
  private TestCatalog catalog;

  @BeforeAll
  void initCatalogManager() {
    GravitinoCatalogManager.create(
        new SparkConf(false), "user", identity -> mock(GravitinoClient.class));
  }

  @AfterAll
  void cleanupCatalogManager() {
    GravitinoCatalogManager.get().close();
  }

  @BeforeEach
  void setUp() throws Exception {
    Catalog gravitinoCatalog = mock(Catalog.class);
    gravitinoTableCatalog = mock(TableCatalog.class);
    sparkCatalog = mock(org.apache.spark.sql.connector.catalog.TableCatalog.class);
    when(gravitinoCatalog.asTableCatalog()).thenReturn(gravitinoTableCatalog);
    catalog = new TestCatalog(gravitinoCatalog, sparkCatalog);
    SparkTypeConverter typeConverter = new SparkTypeConverter();
    setField("sparkTypeConverter", typeConverter);
    setField("sparkTableChangeConverter", new SparkTableChangeConverter(typeConverter));
  }

  @Test
  void testCreateTableCommentIsNotTreatedAsTableProperty() throws Exception {
    // The JDBC converter refuses every table property, as for PostgreSQL, Oracle and SQL Server.
    PropertiesConverter jdbcConverter = JdbcPropertiesConverter.getNoTablePropertiesInstance();
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> jdbcConverter.toGravitinoTableProperties(ImmutableMap.of("comment", "c")));

    StructType schema = new StructType().add("c", DataTypes.IntegerType, true, "column comment");
    catalog.createGravitinoTable(
        IDENT, schema, new Transform[0], ImmutableMap.of("comment", "tbl"));

    ArgumentCaptor<Map<String, String>> properties = ArgumentCaptor.forClass(Map.class);
    verify(gravitinoTableCatalog)
        .createTable(
            eq(GRAVITINO_IDENT),
            any(Column[].class),
            eq("tbl"),
            properties.capture(),
            any(),
            any(),
            any());
    Assertions.assertTrue(properties.getValue().isEmpty());
  }

  @Test
  void testCreateTableDoesNotChangeCallerProperties() throws Exception {
    Map<String, String> sparkProperties = Collections.singletonMap("comment", "tbl");
    StructType schema = new StructType().add("c", DataTypes.IntegerType);
    catalog.createGravitinoTable(IDENT, schema, new Transform[0], sparkProperties);
    // The caller's map is read, never changed.
    Assertions.assertEquals("tbl", sparkProperties.get("comment"));
  }

  @Test
  void testAlterWithoutChangesDoesNotWrite() throws Exception {
    org.apache.gravitino.rel.Table gravitinoTable = mock(org.apache.gravitino.rel.Table.class);
    Table sparkTable = mock(Table.class);
    when(gravitinoTableCatalog.loadTable(
            GRAVITINO_IDENT, ImmutableSet.of(Privilege.Name.SELECT_TABLE)))
        .thenReturn(gravitinoTable);
    when(sparkCatalog.loadTable(IDENT)).thenReturn(sparkTable);

    Assertions.assertSame(sparkTable, catalog.alterTable(IDENT));

    verify(gravitinoTableCatalog, never()).alterTable(any(), any());
  }

  @Test
  void testAddColumnDefaultIsWritten() throws Exception {
    when(sparkCatalog.loadTable(IDENT)).thenReturn(mock(Table.class));

    catalog.alterTable(
        IDENT,
        TableChange.addColumn(
            new String[] {"c_int"},
            DataTypes.IntegerType,
            true,
            null,
            null,
            new ColumnDefaultValue("5", LiteralValue.apply(5, DataTypes.IntegerType))),
        TableChange.addColumn(
            new String[] {"c_str"},
            DataTypes.StringType,
            true,
            "with comment",
            null,
            new ColumnDefaultValue(
                "'abc'",
                LiteralValue.apply(
                    org.apache.spark.unsafe.types.UTF8String.fromString("abc"),
                    DataTypes.StringType))),
        TableChange.addColumn(new String[] {"c_none"}, DataTypes.IntegerType));

    ArgumentCaptor<org.apache.gravitino.rel.TableChange> changes =
        ArgumentCaptor.forClass(org.apache.gravitino.rel.TableChange.class);
    verify(gravitinoTableCatalog).alterTable(eq(GRAVITINO_IDENT), changes.capture());
    org.apache.gravitino.rel.TableChange.AddColumn intColumn =
        (org.apache.gravitino.rel.TableChange.AddColumn) changes.getAllValues().get(0);
    org.apache.gravitino.rel.TableChange.AddColumn stringColumn =
        (org.apache.gravitino.rel.TableChange.AddColumn) changes.getAllValues().get(1);
    org.apache.gravitino.rel.TableChange.AddColumn plainColumn =
        (org.apache.gravitino.rel.TableChange.AddColumn) changes.getAllValues().get(2);
    Assertions.assertEquals(Literals.integerLiteral(5), intColumn.getDefaultValue());
    Assertions.assertEquals(Literals.stringLiteral("abc"), stringColumn.getDefaultValue());
    Assertions.assertEquals("with comment", stringColumn.getComment());
    Assertions.assertEquals(Types.StringType.get(), stringColumn.getDataType());
    Assertions.assertEquals(Column.DEFAULT_VALUE_NOT_SET, plainColumn.getDefaultValue());
    // A new column's default does not need the existing table schema.
    verify(gravitinoTableCatalog, never()).loadTable(GRAVITINO_IDENT);
  }

  @Test
  void testAddColumnUnsupportedDefaultIsRejectedBeforeWriting() {
    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalog.alterTable(
                    IDENT,
                    TableChange.addColumn(
                        new String[] {"c"},
                        DataTypes.IntegerType,
                        true,
                        null,
                        null,
                        new ColumnDefaultValue(
                            "abs(-1)", LiteralValue.apply(1, DataTypes.IntegerType)))));
    Assertions.assertTrue(error.getMessage().contains("abs(-1)"), error.getMessage());
    Mockito.verifyNoInteractions(gravitinoTableCatalog, sparkCatalog);
  }

  private void setField(String name, Object value) throws Exception {
    Field field = BaseCatalog.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(catalog, value);
  }

  private static class TestCatalog extends BaseCatalog {

    private TestCatalog(
        Catalog gravitinoCatalog,
        org.apache.spark.sql.connector.catalog.TableCatalog sparkCatalog) {
      this.gravitinoCatalogClient = gravitinoCatalog;
      this.sparkCatalog = sparkCatalog;
      this.propertiesConverter = JdbcPropertiesConverter.getNoTablePropertiesInstance();
      this.sparkTransformConverter = new SparkTransformConverter(false);
    }

    @Override
    protected org.apache.spark.sql.connector.catalog.TableCatalog createAndInitSparkCatalog(
        String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
      return sparkCatalog;
    }

    @Override
    protected Table createSparkTable(
        Identifier identifier,
        org.apache.gravitino.rel.Table gravitinoTable,
        Table sparkTable,
        org.apache.spark.sql.connector.catalog.TableCatalog sparkCatalog,
        PropertiesConverter propertiesConverter,
        SparkTransformConverter sparkTransformConverter,
        SparkTypeConverter sparkTypeConverter) {
      return sparkTable;
    }

    @Override
    protected PropertiesConverter getPropertiesConverter() {
      return JdbcPropertiesConverter.getNoTablePropertiesInstance();
    }

    @Override
    protected SparkTransformConverter getSparkTransformConverter() {
      return new SparkTransformConverter(false);
    }
  }
}
