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
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.authorization.AuthorizationTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestBaseCatalogAuthorization {

  private Catalog gravitinoCatalog;
  private TableCatalog gravitinoTableCatalog;
  private org.apache.spark.sql.connector.catalog.TableCatalog sparkCatalog;
  private TestCatalog catalog;

  @BeforeAll
  void initCatalogManager() {
    GravitinoCatalogManager.create(() -> mock(GravitinoClient.class));
  }

  @AfterAll
  void cleanupCatalogManager() {
    GravitinoCatalogManager.get().close();
  }

  @BeforeEach
  void setUp() {
    gravitinoCatalog = mock(Catalog.class);
    gravitinoTableCatalog = mock(TableCatalog.class);
    sparkCatalog = mock(org.apache.spark.sql.connector.catalog.TableCatalog.class);
    when(gravitinoCatalog.asTableCatalog()).thenReturn(gravitinoTableCatalog);
    catalog = new TestCatalog(gravitinoCatalog, sparkCatalog);
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
