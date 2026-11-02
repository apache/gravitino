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
package org.apache.gravitino.spark.connector.jdbc.postgresql;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Set;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableWritePrivilege;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Release-branch PostgreSQL writes must use the MODIFY_TABLE metadata load. */
public class TestPostgreSqlWriteAuthorization13 {
  private Catalog client;
  private TableCatalog metadata;
  private org.apache.spark.sql.connector.catalog.TableCatalog spark;
  private TestCatalog catalog;
  private final Identifier identifier = Identifier.of(new String[] {"schema"}, "table");
  private final NameIdentifier metadataIdentifier = NameIdentifier.of("schema", "table");

  @BeforeAll
  static void initializeManager() {
    GravitinoCatalogManager.create("test_metalake", () -> mock(GravitinoClient.class));
  }

  @AfterAll
  static void closeManager() {
    GravitinoCatalogManager.get().close();
  }

  @BeforeEach
  void setUp() {
    client = mock(Catalog.class);
    metadata = mock(TableCatalog.class);
    spark = mock(org.apache.spark.sql.connector.catalog.TableCatalog.class);
    when(client.asTableCatalog()).thenReturn(metadata);
    catalog = new TestCatalog(client, spark);
  }

  @ParameterizedTest
  @EnumSource(TableWritePrivilege.class)
  void testWriteLoadsRequireModifyPrivilege(TableWritePrivilege privilege) throws Exception {
    org.apache.gravitino.rel.Table gravitinoTable = mock(org.apache.gravitino.rel.Table.class);
    Table physical = mock(Table.class);
    when(metadata.loadTable(metadataIdentifier, Set.of(Privilege.Name.MODIFY_TABLE)))
        .thenReturn(gravitinoTable);
    when(spark.loadTable(identifier)).thenReturn(physical);
    Assertions.assertSame(physical, catalog.loadTable(identifier, Set.of(privilege)));
    verify(metadata).loadTable(metadataIdentifier, Set.of(Privilege.Name.MODIFY_TABLE));
    verify(metadata, never()).loadTable(metadataIdentifier);
  }

  @Test
  void testDeniedWriteDoesNotLoadThePhysicalTable() {
    when(metadata.loadTable(metadataIdentifier, Set.of(Privilege.Name.MODIFY_TABLE)))
        .thenThrow(new ForbiddenException("MODIFY_TABLE is required"));
    Assertions.assertThrows(
        ForbiddenException.class,
        () -> catalog.loadTable(identifier, Set.of(TableWritePrivilege.INSERT)));
    verifyNoInteractions(spark);
  }

  private static class TestCatalog extends GravitinoPostgreSqlCatalogSpark35 {
    private TestCatalog(
        Catalog client, org.apache.spark.sql.connector.catalog.TableCatalog delegate) {
      this.gravitinoCatalogClient = client;
      this.sparkCatalog = delegate;
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
  }
}
