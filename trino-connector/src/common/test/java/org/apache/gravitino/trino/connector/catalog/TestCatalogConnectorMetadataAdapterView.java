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
package org.apache.gravitino.trino.connector.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.ConnectorViewDefinition.ViewColumn;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.catalog.hive.HiveDataTypeTransformer;
import org.apache.gravitino.trino.connector.catalog.iceberg.IcebergDataTypeTransformer;
import org.apache.gravitino.trino.connector.metadata.GravitinoColumn;
import org.apache.gravitino.trino.connector.metadata.GravitinoView;
import org.apache.gravitino.trino.connector.util.GeneralDataTypeTransformer;
import org.junit.jupiter.api.Test;

public class TestCatalogConnectorMetadataAdapterView {

  private final CatalogConnectorMetadataAdapter adapter =
      new CatalogConnectorMetadataAdapter(
          List.of(), List.of(), List.of(), new GeneralDataTypeTransformer());

  @Test
  public void testGetViewDefinitionNormalizesCatalogWhenSchemaPresentWithoutCatalog() {
    GravitinoColumn column = new GravitinoColumn(Column.of("id", Types.StringType.get()), 0);
    GravitinoView view =
        new GravitinoView(
            "s", "v1", List.of(column), null, Map.of(), "select 1", null, "default_schema");

    ConnectorViewDefinition definition =
        adapter.getViewDefinition(view, "current_catalog", /* singleMetalakeMode= */ true);

    // Iceberg views may store a default schema without a default catalog; since Trino requires a
    // catalog whenever a schema is present, the current Trino catalog is used.
    assertEquals(Optional.of("current_catalog"), definition.getCatalog());
    assertEquals(Optional.of("default_schema"), definition.getSchema());
  }

  @Test
  public void testGetViewDefinitionRejectsSchemaWithoutCatalogInMultiMetalakeMode() {
    GravitinoColumn column = new GravitinoColumn(Column.of("id", Types.StringType.get()), 0);
    GravitinoView view =
        new GravitinoView(
            "s", "v1", List.of(column), null, Map.of(), "select 1", null, "default_schema");

    TrinoException exception =
        assertThrows(
            TrinoException.class,
            () ->
                adapter.getViewDefinition(
                    view, "current_catalog", /* singleMetalakeMode= */ false));
    assertEquals(
        GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION.toErrorCode(), exception.getErrorCode());
  }

  @Test
  public void testGetViewDefinitionKeepsNullSchemaWhenCatalogAbsent() {
    GravitinoColumn column = new GravitinoColumn(Column.of("id", Types.StringType.get()), 0);
    GravitinoView view =
        new GravitinoView("s", "v1", List.of(column), null, Map.of(), "select 1", null, null);

    ConnectorViewDefinition definition =
        adapter.getViewDefinition(view, "current_catalog", /* singleMetalakeMode= */ false);

    assertEquals(Optional.empty(), definition.getCatalog());
    assertEquals(Optional.empty(), definition.getSchema());
  }

  @Test
  public void testGetViewDefinitionIsRunAsInvokerWhenNoOwnerProperty() {
    GravitinoColumn column = new GravitinoColumn(Column.of("id", Types.StringType.get()), 0);
    GravitinoView view =
        new GravitinoView("s", "v1", List.of(column), null, Map.of(), "select 1", null, null);

    ConnectorViewDefinition definition =
        adapter.getViewDefinition(view, "current_catalog", /* singleMetalakeMode= */ true);

    assertEquals(Optional.empty(), definition.getOwner());
    assertEquals(true, definition.isRunAsInvoker());
  }

  @Test
  public void testGetViewDefinitionIsRunAsDefinerWhenOwnerPropertyPresent() {
    GravitinoColumn column = new GravitinoColumn(Column.of("id", Types.StringType.get()), 0);
    GravitinoView view =
        new GravitinoView(
            "s",
            "v1",
            List.of(column),
            null,
            Map.of("trino.internal.view.owner", "alice"),
            "select 1",
            null,
            null);

    ConnectorViewDefinition definition =
        adapter.getViewDefinition(view, "current_catalog", /* singleMetalakeMode= */ true);

    assertEquals(Optional.of("alice"), definition.getOwner());
    assertEquals(false, definition.isRunAsInvoker());
  }

  @Test
  public void testCreateViewPersistsOwnerAsReservedProperty() {
    ViewColumn column = new ViewColumn("id", VarcharType.VARCHAR.getTypeId(), Optional.empty());
    ConnectorViewDefinition definition =
        new ConnectorViewDefinition(
            "select 1",
            Optional.empty(),
            Optional.empty(),
            List.of(column),
            Optional.empty(),
            Optional.of("alice"),
            false,
            List.of());

    GravitinoView view = adapter.createView(new SchemaTableName("s", "v1"), definition, Map.of());

    assertEquals("alice", view.getProperties().get("trino.internal.view.owner"));
  }

  @Test
  public void testCreateViewIgnoresCallerSuppliedOwnerProperty() {
    ViewColumn column = new ViewColumn("id", VarcharType.VARCHAR.getTypeId(), Optional.empty());
    ConnectorViewDefinition definition =
        new ConnectorViewDefinition(
            "select 1",
            Optional.empty(),
            Optional.empty(),
            List.of(column),
            Optional.empty(),
            Optional.empty(),
            true,
            List.of());

    GravitinoView view =
        adapter.createView(
            new SchemaTableName("s", "v1"),
            definition,
            Map.of("trino.internal.view.owner", "spoofed"));

    assertEquals(null, view.getProperties().get("trino.internal.view.owner"));
  }

  @Test
  public void testCreateViewRejectsNonEmptyPath() {
    ViewColumn column = new ViewColumn("id", VarcharType.VARCHAR.getTypeId(), Optional.empty());
    ConnectorViewDefinition definition =
        new ConnectorViewDefinition(
            "select 1",
            Optional.empty(),
            Optional.empty(),
            List.of(column),
            Optional.empty(),
            Optional.empty(),
            true,
            List.of(new CatalogSchemaName("c", "s")));

    TrinoException exception =
        assertThrows(
            TrinoException.class,
            () -> adapter.createView(new SchemaTableName("s", "v1"), definition, Map.of()));
    assertEquals(
        GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION.toErrorCode(), exception.getErrorCode());
  }

  @Test
  public void testCreateViewAllowsIcebergVarcharWithLength() {
    // A view's output columns are never physically stored, so Iceberg's "VARCHAR must have no
    // length" table-column restriction must not apply to them (e.g. SELECT 'x' AS c naturally
    // produces varchar(1)); IcebergDataTypeTransformer.getGravitinoType() would otherwise reject
    // it.
    CatalogConnectorMetadataAdapter icebergAdapter =
        new CatalogConnectorMetadataAdapter(
            List.of(), List.of(), List.of(), new IcebergDataTypeTransformer());
    ViewColumn column =
        new ViewColumn("c", VarcharType.createVarcharType(1).getTypeId(), Optional.empty());
    ConnectorViewDefinition definition =
        new ConnectorViewDefinition(
            "select 'x' as c",
            Optional.empty(),
            Optional.empty(),
            List.of(column),
            Optional.empty(),
            Optional.empty(),
            true,
            List.of());

    GravitinoView view =
        icebergAdapter.createView(new SchemaTableName("s", "v1"), definition, Map.of());

    assertEquals(Types.VarCharType.of(1), view.getColumns().get(0).getType());
  }

  @Test
  public void testViewRoundTripsHiveTimestampWithTimeZone() {
    // A view's output columns are never physically stored, so Hive's "no timestamp with time
    // zone" table-column restriction must not apply to them; HiveDataTypeTransformer previously
    // accepted this type on create (falling through to the unrestricted base transformer) but
    // rejected it on getTrinoType(), so the view could be created but never reloaded.
    CatalogConnectorMetadataAdapter hiveAdapter =
        new CatalogConnectorMetadataAdapter(
            List.of(), List.of(), List.of(), new HiveDataTypeTransformer());
    ViewColumn column =
        new ViewColumn(
            "c",
            TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3).getTypeId(),
            Optional.empty());
    ConnectorViewDefinition definition =
        new ConnectorViewDefinition(
            "select current_timestamp as c",
            Optional.empty(),
            Optional.empty(),
            List.of(column),
            Optional.empty(),
            Optional.empty(),
            true,
            List.of());

    GravitinoView view =
        hiveAdapter.createView(new SchemaTableName("s", "v1"), definition, Map.of());
    assertEquals(Types.TimestampType.withTimeZone(3), view.getColumns().get(0).getType());

    ConnectorViewDefinition reloaded =
        hiveAdapter.getViewDefinition(view, "current_catalog", /* singleMetalakeMode= */ true);
    assertEquals(
        TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3).getTypeId(),
        reloaded.getColumns().get(0).getType());
  }
}
