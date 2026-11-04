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

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTableChangeConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.authorization.AuthorizationTable;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestBaseCatalogAuthorization {

  private Catalog gravitinoCatalog;
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
    gravitinoCatalog = mock(Catalog.class);
    gravitinoTableCatalog = mock(TableCatalog.class);
    sparkCatalog = mock(org.apache.spark.sql.connector.catalog.TableCatalog.class);
    when(gravitinoCatalog.asTableCatalog()).thenReturn(gravitinoTableCatalog);
    catalog = new TestCatalog(gravitinoCatalog, sparkCatalog);
    Field converter = BaseCatalog.class.getDeclaredField("sparkTableChangeConverter");
    converter.setAccessible(true);
    converter.set(catalog, new SparkTableChangeConverter(new SparkTypeConverter()));
  }

  @AfterEach
  void clearDeniedTables() {
    AuthorizationTable.clear();
  }

  @Test
  void testLoadTablePassesSelectTablePrivilege() throws Exception {
    Identifier identifier = Identifier.of(new String[] {"schema"}, "table_a");
    NameIdentifier gravitinoIdentifier = NameIdentifier.of("schema", "table_a");
    org.apache.gravitino.rel.Table gravitinoTable = mock(org.apache.gravitino.rel.Table.class);
    Table sparkTable = mock(Table.class);
    when(gravitinoTableCatalog.loadTable(
            eq(gravitinoIdentifier), eq(ImmutableSet.of(Privilege.Name.SELECT_TABLE))))
        .thenReturn(gravitinoTable);
    when(sparkCatalog.loadTable(identifier)).thenReturn(sparkTable);

    assertSame(sparkTable, catalog.loadTable(identifier));

    verify(gravitinoTableCatalog)
        .loadTable(gravitinoIdentifier, ImmutableSet.of(Privilege.Name.SELECT_TABLE));
  }

  @Test
  void testDeniedReadReturnsMarkedSparkTable() throws Exception {
    Identifier identifier = Identifier.of(new String[] {"schema"}, "table_a");
    NameIdentifier gravitinoIdentifier = NameIdentifier.of("schema", "table_a");
    when(gravitinoTableCatalog.loadTable(
            eq(gravitinoIdentifier), eq(ImmutableSet.of(Privilege.Name.SELECT_TABLE))))
        .thenThrow(new ForbiddenException("denied"));

    Table result = catalog.loadTable(identifier);

    // The underlying Spark table is never loaded for a denied table; a placeholder is returned and
    // the required privileges are collected for RequiredPrivilegesCheck to report during analysis.
    assertTrue(result instanceof AuthorizationTable);
    Optional<ForbiddenException> failure = AuthorizationTable.drainFailure();
    assertTrue(failure.isPresent());
    assertTrue(failure.get().getMessage().contains("table_a: SELECT_TABLE"));
  }

  @Test
  void testLoadTableForWritingPassesModifyTablePrivilege() throws Exception {
    Identifier identifier = Identifier.of(new String[] {"schema"}, "table_a");
    NameIdentifier gravitinoIdentifier = NameIdentifier.of("schema", "table_a");
    org.apache.gravitino.rel.Table gravitinoTable = mock(org.apache.gravitino.rel.Table.class);
    Table sparkTable = mock(Table.class);
    when(gravitinoTableCatalog.loadTable(
            eq(gravitinoIdentifier), eq(ImmutableSet.of(Privilege.Name.MODIFY_TABLE))))
        .thenReturn(gravitinoTable);
    when(sparkCatalog.loadTable(identifier)).thenReturn(sparkTable);

    assertSame(sparkTable, catalog.loadForWriting(identifier));

    verify(gravitinoTableCatalog)
        .loadTable(gravitinoIdentifier, ImmutableSet.of(Privilege.Name.MODIFY_TABLE));
  }

  @Test
  void testAlterTimestampDefaultLoadsTargetSchemaBeforeWriting() throws Exception {
    Identifier identifier = Identifier.of(new String[] {"schema"}, "table_a");
    NameIdentifier gravitinoIdentifier = NameIdentifier.of("schema", "table_a");
    org.apache.gravitino.rel.Table table = mock(org.apache.gravitino.rel.Table.class);
    when(gravitinoTableCatalog.loadTable(gravitinoIdentifier)).thenReturn(table);
    when(table.columns())
        .thenReturn(new Column[] {Column.of("ts", Types.TimestampType.withoutTimeZone(6), null)});
    Table sparkTable = mock(Table.class);
    when(sparkCatalog.loadTable(identifier)).thenReturn(sparkTable);
    SQLConf conf = new SQLConf();
    conf.setConfString("spark.sql.session.timeZone", "Asia/Shanghai");
    SQLConf.withExistingConf(
        conf,
        () -> {
          Assertions.assertDoesNotThrow(
              () ->
                  catalog.alterTable(
                      identifier,
                      TableChange.updateColumnDefaultValue(
                          new String[] {"ts"}, "TIMESTAMP_LTZ '2026-11-04 12:34:56.123456'")));
          return null;
        });
    ArgumentCaptor<org.apache.gravitino.rel.TableChange> change =
        ArgumentCaptor.forClass(org.apache.gravitino.rel.TableChange.class);
    verify(gravitinoTableCatalog).alterTable(eq(gravitinoIdentifier), change.capture());
    Assertions.assertEquals(
        Literals.timestampLiteral("2026-11-04T12:34:56.123456"),
        ((org.apache.gravitino.rel.TableChange.UpdateColumnDefaultValue) change.getValue())
            .getNewDefaultValue());
    verify(gravitinoTableCatalog).loadTable(gravitinoIdentifier);
  }

  @Test
  void testTimestampDefaultRejectsSchemaChangingBatchBeforeWriting() {
    Identifier identifier = Identifier.of(new String[] {"schema"}, "table_a");
    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalog.alterTable(
                    identifier,
                    TableChange.updateColumnDefaultValue(
                        new String[] {"ts"}, "TIMESTAMP_LTZ '2026-11-04 12:34:00'"),
                    TableChange.updateColumnType(new String[] {"ts"}, DataTypes.TimestampNTZType)));
    Assertions.assertTrue(error.getMessage().contains("cannot be combined"));
    Mockito.verifyNoInteractions(gravitinoTableCatalog, sparkCatalog);
  }

  @Test
  void testTimestampSchemaReadFailurePreventsWritingAndPreservesCause() {
    Identifier identifier = Identifier.of(new String[] {"schema"}, "table_a");
    NameIdentifier gravitinoIdentifier = NameIdentifier.of("schema", "table_a");
    ForbiddenException denied = new ForbiddenException("denied schema read");
    when(gravitinoTableCatalog.loadTable(gravitinoIdentifier)).thenThrow(denied);
    Assertions.assertSame(
        denied,
        Assertions.assertThrows(
            ForbiddenException.class,
            () ->
                catalog.alterTable(
                    identifier,
                    TableChange.updateColumnDefaultValue(
                        new String[] {"ts"}, "TIMESTAMP_LTZ '2026-11-04 12:34:00'"))));
    verify(gravitinoTableCatalog).loadTable(gravitinoIdentifier);
    Mockito.verifyNoMoreInteractions(gravitinoTableCatalog);
    Mockito.verifyNoInteractions(sparkCatalog);
  }

  @Test
  void testNonTimestampDefaultDoesNotReadSchema() throws Exception {
    Identifier identifier = Identifier.of(new String[] {"schema"}, "table_a");
    NameIdentifier gravitinoIdentifier = NameIdentifier.of("schema", "table_a");
    catalog.alterTable(identifier, TableChange.updateColumnDefaultValue(new String[] {"ts"}, "42"));
    verify(gravitinoTableCatalog, Mockito.never()).loadTable(gravitinoIdentifier);
  }

  @Test
  void testTimestampDefaultsShareOneSchemaRead() throws Exception {
    Identifier identifier = Identifier.of(new String[] {"schema"}, "table_a");
    NameIdentifier gravitinoIdentifier = NameIdentifier.of("schema", "table_a");
    org.apache.gravitino.rel.Table table = mock(org.apache.gravitino.rel.Table.class);
    when(gravitinoTableCatalog.loadTable(gravitinoIdentifier)).thenReturn(table);
    when(table.columns())
        .thenReturn(new Column[] {Column.of("ts", Types.TimestampType.withoutTimeZone(), null)});
    catalog.alterTable(
        identifier,
        TableChange.updateColumnDefaultValue(
            new String[] {"ts"}, "TIMESTAMP_NTZ '2026-11-04 12:34:00'"),
        TableChange.updateColumnDefaultValue(
            new String[] {"ts"}, "TIMESTAMP_NTZ '2026-11-04 00:00:00'"));
    verify(gravitinoTableCatalog).loadTable(gravitinoIdentifier);
    verify(table).columns();
  }

  private static class TestCatalog extends BaseCatalog {

    private TestCatalog(
        Catalog gravitinoCatalog,
        org.apache.spark.sql.connector.catalog.TableCatalog sparkCatalog) {
      this.gravitinoCatalogClient = gravitinoCatalog;
      this.sparkCatalog = sparkCatalog;
    }

    private Table loadForWriting(Identifier identifier) throws Exception {
      return loadTableForWriting(identifier);
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
      return mock(PropertiesConverter.class);
    }

    @Override
    protected SparkTransformConverter getSparkTransformConverter() {
      return mock(SparkTransformConverter.class);
    }
  }
}
