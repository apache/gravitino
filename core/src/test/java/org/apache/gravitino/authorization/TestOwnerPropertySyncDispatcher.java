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
package org.apache.gravitino.authorization;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestOwnerPropertySyncDispatcher {

  private static final String METALAKE = "m";
  private static final MetadataObject TABLE =
      MetadataObjects.of(Arrays.asList("cat", "ns", "t"), MetadataObject.Type.TABLE);
  private static final MetadataObject SCHEMA =
      MetadataObjects.of(Arrays.asList("cat", "ns"), MetadataObject.Type.SCHEMA);
  private static final NameIdentifier TABLE_IDENT = NameIdentifier.of(METALAKE, "cat", "ns", "t");
  private static final NameIdentifier SCHEMA_IDENT = NameIdentifier.of(METALAKE, "cat", "ns");

  private final GravitinoEnv env = GravitinoEnv.getInstance();
  private Object previousCatalogs;
  private Object previousSchemas;
  private Object previousTables;
  private OwnerDispatcher delegate;
  private Catalog catalog;
  private SchemaDispatcher schemas;
  private TableDispatcher tables;
  private OwnerPropertySyncDispatcher dispatcher;

  @BeforeEach
  public void setUp() throws IllegalAccessException {
    previousCatalogs = FieldUtils.readField(env, "internalCatalogDispatcher", true);
    previousSchemas = FieldUtils.readField(env, "internalSchemaDispatcher", true);
    previousTables = FieldUtils.readField(env, "internalTableDispatcher", true);
    CatalogDispatcher catalogs = mock(CatalogDispatcher.class);
    catalog = mock(Catalog.class);
    when(catalogs.loadCatalog(NameIdentifier.of(METALAKE, "cat"))).thenReturn(catalog);
    when(catalog.provider()).thenReturn("lakehouse-iceberg");
    schemas = mock(SchemaDispatcher.class);
    tables = mock(TableDispatcher.class);
    FieldUtils.writeField(env, "internalCatalogDispatcher", catalogs, true);
    FieldUtils.writeField(env, "internalSchemaDispatcher", schemas, true);
    FieldUtils.writeField(env, "internalTableDispatcher", tables, true);
    delegate = mock(OwnerDispatcher.class);
    dispatcher = new OwnerPropertySyncDispatcher(delegate);
  }

  @AfterEach
  public void tearDown() throws IllegalAccessException {
    FieldUtils.writeField(env, "internalCatalogDispatcher", previousCatalogs, true);
    FieldUtils.writeField(env, "internalSchemaDispatcher", previousSchemas, true);
    FieldUtils.writeField(env, "internalTableDispatcher", previousTables, true);
  }

  @Test
  public void testTransferRewritesTheOwnerPropertyOfAnIcebergTable() {
    Table table = mock(Table.class);
    when(table.properties()).thenReturn(ImmutableMap.of("owner", "admin"));
    when(tables.loadTable(TABLE_IDENT)).thenReturn(table);

    dispatcher.setOwner(METALAKE, TABLE, "writer", Owner.Type.USER);

    verify(delegate).setOwner(METALAKE, TABLE, "writer", Owner.Type.USER);
    verify(tables).alterTable(TABLE_IDENT, TableChange.setProperty("owner", "writer"));
  }

  @Test
  public void testTransferRewritesTheOwnerPropertyOfIcebergNamespaces() {
    Schema schema = mock(Schema.class);
    when(schema.properties()).thenReturn(ImmutableMap.of("owner", "admin"));
    when(schemas.loadSchema(SCHEMA_IDENT)).thenReturn(schema);

    dispatcher.setOwners(METALAKE, Collections.singletonList(SCHEMA), "writers", Owner.Type.GROUP);

    verify(schemas).alterSchema(SCHEMA_IDENT, SchemaChange.setProperty("owner", "writers"));
  }

  @Test
  public void testObjectsWithoutThePropertyOrOutsideIcebergAreLeftAlone() {
    Table table = mock(Table.class);
    when(table.properties()).thenReturn(ImmutableMap.of("k", "v"));
    when(tables.loadTable(TABLE_IDENT)).thenReturn(table);
    dispatcher.setOwner(METALAKE, TABLE, "writer", Owner.Type.USER);

    when(catalog.provider()).thenReturn("hive");
    when(table.properties()).thenReturn(ImmutableMap.of("owner", "admin"));
    dispatcher.setOwner(METALAKE, TABLE, "writer", Owner.Type.USER);

    verify(tables, never()).alterTable(any(), any());
  }

  @Test
  public void testAFailedPropertyUpdateDoesNotFailTheTransfer() {
    Table table = mock(Table.class);
    when(table.properties()).thenReturn(ImmutableMap.of("owner", "admin"));
    when(tables.loadTable(TABLE_IDENT)).thenReturn(table);
    when(tables.alterTable(any(), any())).thenThrow(new IllegalStateException("catalog down"));

    Assertions.assertDoesNotThrow(
        () -> dispatcher.setOwner(METALAKE, TABLE, "writer", Owner.Type.USER));
    verify(delegate).setOwner(METALAKE, TABLE, "writer", Owner.Type.USER);
  }

  @Test
  public void testAFailedTransferDoesNotTouchTheProperty() {
    Mockito.doThrow(new IllegalArgumentException("no such user"))
        .when(delegate)
        .setOwner(METALAKE, TABLE, "nobody", Owner.Type.USER);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> dispatcher.setOwner(METALAKE, TABLE, "nobody", Owner.Type.USER));
    verify(tables, never()).loadTable(any());
  }
}
