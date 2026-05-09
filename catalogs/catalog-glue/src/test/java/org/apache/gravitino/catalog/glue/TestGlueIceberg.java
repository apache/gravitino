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
package org.apache.gravitino.catalog.glue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.IcebergSchema;
import software.amazon.awssdk.services.glue.model.IcebergStructField;
import software.amazon.awssdk.services.glue.model.IcebergTableUpdate;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;

class TestGlueIceberg {

  private static final String DB = "mydb";
  private static final String TABLE = "ice1";
  private static final String LOCATION = "s3://my-bucket/warehouse/ice1";

  private GlueClient mockClient;
  private GlueCatalogOperations ops;

  @BeforeEach
  void setup() {
    mockClient = mock(GlueClient.class);
    ops = new GlueCatalogOperations();
    ops.glueClient = mockClient;
    ops.catalogId = null;
    ops.tableFormatFilter = null;
    ops.defaultTableFormat = GlueConstants.DEFAULT_TABLE_FORMAT_VALUE;
  }

  // ---------------------------------------------------------------------------
  // isIcebergTable
  // ---------------------------------------------------------------------------

  @Test
  void testIsIcebergTable_withIcebergType() {
    Table t = Table.builder().parameters(Map.of("table_type", "ICEBERG")).build();
    assertTrue(GlueIcebergHelper.isIcebergTable(t));
  }

  @Test
  void testIsIcebergTable_caseInsensitive() {
    Table t = Table.builder().parameters(Map.of("table_type", "iceberg")).build();
    assertTrue(GlueIcebergHelper.isIcebergTable(t));
  }

  @Test
  void testIsIcebergTable_hiveTable() {
    Table t = Table.builder().parameters(Map.of("table_type", "HIVE")).build();
    assertFalse(GlueIcebergHelper.isIcebergTable(t));
  }

  @Test
  void testIsIcebergTable_noParameters() {
    Table t = Table.builder().build();
    assertFalse(GlueIcebergHelper.isIcebergTable(t));
  }

  // ---------------------------------------------------------------------------
  // hiveTypeToDoc
  // ---------------------------------------------------------------------------

  @Test
  void testHiveTypeToDoc_primitives() {
    assertEquals("long", GlueIcebergHelper.hiveTypeToDoc("bigint").asString());
    assertEquals("int", GlueIcebergHelper.hiveTypeToDoc("int").asString());
    assertEquals("int", GlueIcebergHelper.hiveTypeToDoc("smallint").asString());
    assertEquals("float", GlueIcebergHelper.hiveTypeToDoc("float").asString());
    assertEquals("double", GlueIcebergHelper.hiveTypeToDoc("double").asString());
    assertEquals("boolean", GlueIcebergHelper.hiveTypeToDoc("boolean").asString());
    assertEquals("binary", GlueIcebergHelper.hiveTypeToDoc("binary").asString());
    assertEquals("date", GlueIcebergHelper.hiveTypeToDoc("date").asString());
    assertEquals("timestamp", GlueIcebergHelper.hiveTypeToDoc("timestamp").asString());
    assertEquals("string", GlueIcebergHelper.hiveTypeToDoc("string").asString());
  }

  @Test
  void testHiveTypeToDoc_decimal() {
    var doc = GlueIcebergHelper.hiveTypeToDoc("decimal(18,2)");
    var m = doc.asMap();
    assertEquals("decimal", m.get("type").asString());
    assertEquals(18, m.get("precision").asNumber().intValue());
    assertEquals(2, m.get("scale").asNumber().intValue());
  }

  @Test
  void testHiveTypeToDoc_varchar() {
    assertEquals("string", GlueIcebergHelper.hiveTypeToDoc("varchar(255)").asString());
  }

  // ---------------------------------------------------------------------------
  // gravitinoTypeToDoc
  // ---------------------------------------------------------------------------

  @Test
  void testGravitinoTypeToDoc_primitives() {
    assertEquals("long", GlueIcebergHelper.gravitinoTypeToDoc(Types.LongType.get()).asString());
    assertEquals("int", GlueIcebergHelper.gravitinoTypeToDoc(Types.IntegerType.get()).asString());
    assertEquals("float", GlueIcebergHelper.gravitinoTypeToDoc(Types.FloatType.get()).asString());
    assertEquals("double", GlueIcebergHelper.gravitinoTypeToDoc(Types.DoubleType.get()).asString());
    assertEquals(
        "boolean", GlueIcebergHelper.gravitinoTypeToDoc(Types.BooleanType.get()).asString());
    assertEquals("string", GlueIcebergHelper.gravitinoTypeToDoc(Types.StringType.get()).asString());
    assertEquals("date", GlueIcebergHelper.gravitinoTypeToDoc(Types.DateType.get()).asString());
    assertEquals(
        "timestamp",
        GlueIcebergHelper.gravitinoTypeToDoc(Types.TimestampType.withoutTimeZone()).asString());
    assertEquals(
        "timestamptz",
        GlueIcebergHelper.gravitinoTypeToDoc(Types.TimestampType.withTimeZone()).asString());
    assertEquals("uuid", GlueIcebergHelper.gravitinoTypeToDoc(Types.UUIDType.get()).asString());
  }

  @Test
  void testGravitinoTypeToDoc_decimal() {
    var doc = GlueIcebergHelper.gravitinoTypeToDoc(Types.DecimalType.of(10, 3));
    var m = doc.asMap();
    assertEquals("decimal", m.get("type").asString());
    assertEquals(10, m.get("precision").asNumber().intValue());
    assertEquals(3, m.get("scale").asNumber().intValue());
  }

  @Test
  void testGravitinoTypeToDoc_unsupported() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> GlueIcebergHelper.gravitinoTypeToDoc(Types.NullType.get()));
  }

  // ---------------------------------------------------------------------------
  // buildIcebergTableUpdates
  // ---------------------------------------------------------------------------

  @Test
  void testBuildIcebergTableUpdates_addColumn() {
    Table raw = icebergTable(icebergColumn("id", "long", 1, false));

    TableChange add = TableChange.addColumn(new String[] {"score"}, Types.FloatType.get(), true);
    List<IcebergTableUpdate> updates = GlueIcebergHelper.buildIcebergTableUpdates(raw, add);

    assertEquals(1, updates.size());
    IcebergSchema schema = updates.get(0).schema();
    assertNotNull(schema);
    assertEquals(2, schema.fields().size());
    IcebergStructField newField = schema.fields().get(1);
    assertEquals("score", newField.name());
    assertEquals("float", newField.type().asString());
    assertEquals(2, newField.id());
    assertFalse(newField.required());
  }

  @Test
  void testBuildIcebergTableUpdates_deleteColumn() {
    Table raw =
        icebergTable(
            icebergColumn("id", "long", 1, false), icebergColumn("name", "string", 2, true));

    TableChange delete = TableChange.deleteColumn(new String[] {"name"}, true);
    List<IcebergTableUpdate> updates = GlueIcebergHelper.buildIcebergTableUpdates(raw, delete);

    assertEquals(1, updates.size());
    IcebergSchema schema = updates.get(0).schema();
    assertNotNull(schema);
    assertEquals(1, schema.fields().size());
    assertEquals("id", schema.fields().get(0).name());
  }

  @Test
  void testBuildIcebergTableUpdates_renameColumn() {
    Table raw = icebergTable(icebergColumn("old_name", "string", 1, true));

    TableChange rename = TableChange.renameColumn(new String[] {"old_name"}, "new_name");
    List<IcebergTableUpdate> updates = GlueIcebergHelper.buildIcebergTableUpdates(raw, rename);

    assertEquals(1, updates.size());
    IcebergSchema schema = updates.get(0).schema();
    assertNotNull(schema);
    assertEquals("new_name", schema.fields().get(0).name());
    assertEquals(1, schema.fields().get(0).id()); // ID preserved
  }

  @Test
  void testBuildIcebergTableUpdates_setProperty() {
    Table raw = icebergTable();

    TableChange set = TableChange.setProperty("write.format.default", "parquet");
    List<IcebergTableUpdate> updates = GlueIcebergHelper.buildIcebergTableUpdates(raw, set);

    assertEquals(1, updates.size());
    Map<String, String> props = updates.get(0).properties();
    assertNotNull(props);
    assertEquals("parquet", props.get("write.format.default"));
  }

  @Test
  void testBuildIcebergTableUpdates_removePropertyThrows() {
    Table raw = icebergTable();
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            GlueIcebergHelper.buildIcebergTableUpdates(
                raw, TableChange.removeProperty("some.prop")));
  }

  @Test
  void testBuildIcebergTableUpdates_schemaIdIncrement() {
    Table raw =
        Table.builder()
            .parameters(Map.of("table_type", "ICEBERG", "current-schema-id", "3"))
            .storageDescriptor(StorageDescriptor.builder().build())
            .build();

    TableChange add = TableChange.addColumn(new String[] {"ts"}, Types.DateType.get(), true);
    List<IcebergTableUpdate> updates = GlueIcebergHelper.buildIcebergTableUpdates(raw, add);

    IcebergSchema schema = updates.get(0).schema();
    assertEquals(4, schema.schemaId());
  }

  @Test
  void testBuildIcebergTableUpdates_mixedChanges() {
    Table raw = icebergTable(icebergColumn("id", "long", 1, false));

    List<IcebergTableUpdate> updates =
        GlueIcebergHelper.buildIcebergTableUpdates(
            raw,
            TableChange.addColumn(new String[] {"ts"}, Types.DateType.get(), true),
            TableChange.setProperty("write.target-file-size-bytes", "134217728"));

    assertEquals(2, updates.size());
    IcebergSchema schema = updates.get(0).schema();
    assertNotNull(schema);
    assertEquals(2, schema.fields().size());
    assertEquals("ts", schema.fields().get(1).name());
    Map<String, String> props = updates.get(1).properties();
    assertNotNull(props);
    assertEquals("134217728", props.get("write.target-file-size-bytes"));
  }

  @Test
  void testBuildIcebergTableUpdates_updateColumnType() {
    Table raw = icebergTable(icebergColumn("id", "int", 1, false));

    List<IcebergTableUpdate> updates =
        GlueIcebergHelper.buildIcebergTableUpdates(
            raw, TableChange.updateColumnType(new String[] {"id"}, Types.LongType.get()));

    IcebergSchema schema = updates.get(0).schema();
    assertEquals("long", schema.fields().get(0).type().asString());
    assertEquals(1, schema.fields().get(0).id()); // field ID preserved
  }

  @Test
  void testBuildIcebergTableUpdates_emptyChanges() {
    Table raw = icebergTable(icebergColumn("id", "long", 1, false));

    List<IcebergTableUpdate> updates = GlueIcebergHelper.buildIcebergTableUpdates(raw);

    assertEquals(0, updates.size());
  }

  @Test
  void testBuildIcebergTableUpdates_nonSequentialFieldIds() {
    // Simulate a table after column deletions: IDs 1, 5, 10 (non-contiguous)
    Table raw =
        icebergTable(
            icebergColumn("a", "long", 1, false),
            icebergColumn("b", "string", 5, true),
            icebergColumn("c", "date", 10, true));

    List<IcebergTableUpdate> updates =
        GlueIcebergHelper.buildIcebergTableUpdates(
            raw, TableChange.addColumn(new String[] {"d"}, Types.IntegerType.get(), true));

    IcebergSchema schema = updates.get(0).schema();
    IcebergStructField newField = schema.fields().get(3);
    assertEquals("d", newField.name());
    assertEquals(11, newField.id()); // max(1,5,10) + 1 = 11, not fields.size()+1 = 4
  }

  @Test
  void testBuildIcebergTableUpdates_columnNotFoundThrows() {
    Table raw = icebergTable(icebergColumn("id", "long", 1, false));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            GlueIcebergHelper.buildIcebergTableUpdates(
                raw, TableChange.renameColumn(new String[] {"nonexistent"}, "new_name")));
  }

  @Test
  void testHiveTypeToDoc_unknownTypeFallsBackToString() {
    // Complex types (array, map, struct) are not supported — should warn and return string
    assertEquals("string", GlueIcebergHelper.hiveTypeToDoc("array<string>").asString());
  }

  @Test
  void testHiveTypeToDoc_malformedDecimalFallsBackToString() {
    assertEquals("string", GlueIcebergHelper.hiveTypeToDoc("decimal(abc,2)").asString());
  }

  // ---------------------------------------------------------------------------
  // createTable Iceberg routing
  // ---------------------------------------------------------------------------

  @Test
  void testCreateTable_icebergRoutesOpenTableFormatInput() {
    Table created =
        Table.builder()
            .name(TABLE)
            .parameters(
                Map.of(
                    "table_type",
                    "ICEBERG",
                    "metadata_location",
                    LOCATION + "/metadata/00000.metadata.json"))
            .storageDescriptor(StorageDescriptor.builder().build())
            .build();

    when(mockClient.createTable(any(CreateTableRequest.class)))
        .thenReturn(CreateTableResponse.builder().build());
    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(created).build());

    NameIdentifier ident = NameIdentifier.of("cat", "ns", DB, TABLE);
    Column[] cols = {
      GlueColumn.builder().withName("id").withType(Types.LongType.get()).withNullable(false).build()
    };

    ops.createTable(
        ident,
        cols,
        "iceberg table",
        Map.of(GlueConstants.TABLE_FORMAT, "iceberg", GlueConstants.LOCATION, LOCATION),
        new Transform[0],
        Distributions.NONE,
        null,
        Indexes.EMPTY_INDEXES);

    ArgumentCaptor<CreateTableRequest> captor = ArgumentCaptor.forClass(CreateTableRequest.class);
    verify(mockClient).createTable(captor.capture());
    CreateTableRequest req = captor.getValue();
    assertNotNull(
        req.openTableFormatInput(), "openTableFormatInput must be set for Iceberg tables");
    assertNotNull(req.openTableFormatInput().icebergInput());
  }

  // ---------------------------------------------------------------------------
  // alterTable Iceberg routing
  // ---------------------------------------------------------------------------

  @Test
  void testAlterTable_icebergRoutesUpdateOpenTableFormatInput() {
    Table rawTable = icebergTable(icebergColumn("id", "long", 1, false));

    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(rawTable).build());
    when(mockClient.updateTable(any(UpdateTableRequest.class)))
        .thenReturn(UpdateTableResponse.builder().build());

    NameIdentifier ident = NameIdentifier.of("cat", "ns", DB, TABLE);
    ops.alterTable(
        ident, TableChange.addColumn(new String[] {"score"}, Types.DoubleType.get(), true));

    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    verify(mockClient).updateTable(captor.capture());
    UpdateTableRequest req = captor.getValue();
    assertNotNull(
        req.updateOpenTableFormatInput(),
        "updateOpenTableFormatInput must be set for Iceberg alter");
    assertNotNull(req.updateOpenTableFormatInput().updateIcebergInput());
  }

  @Test
  void testCreateTable_icebergMissingLocationThrows() {
    NameIdentifier ident = NameIdentifier.of("cat", "ns", DB, TABLE);
    Column[] cols = {
      GlueColumn.builder().withName("id").withType(Types.LongType.get()).withNullable(false).build()
    };

    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.createTable(
                ident,
                cols,
                "iceberg table",
                Map.of(GlueConstants.TABLE_FORMAT, "iceberg"), // missing location
                new Transform[0],
                Distributions.NONE,
                null,
                Indexes.EMPTY_INDEXES));
  }

  @Test
  void testAlterTable_icebergRenameThrows() {
    Table rawTable = icebergTable(icebergColumn("id", "long", 1, false));

    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(rawTable).build());

    NameIdentifier ident = NameIdentifier.of("cat", "ns", DB, TABLE);
    assertThrows(
        UnsupportedOperationException.class,
        () -> ops.alterTable(ident, TableChange.rename("new_table_name")));
  }

  @Test
  void testHiveTypeToDoc_nullFallsBackToString() {
    assertEquals("string", GlueIcebergHelper.hiveTypeToDoc(null).asString());
  }

  @Test
  void testBuildIcebergTableUpdates_updateColumnNullability() {
    Table raw = icebergTable(icebergColumn("id", "long", 1, true));

    List<IcebergTableUpdate> updates =
        GlueIcebergHelper.buildIcebergTableUpdates(
            raw, TableChange.updateColumnNullability(new String[] {"id"}, false));

    IcebergSchema schema = updates.get(0).schema();
    assertTrue(schema.fields().get(0).required());
  }

  @Test
  void testBuildIcebergTableUpdates_updateColumnComment() {
    Table raw = icebergTable(icebergColumn("id", "long", 1, false));

    List<IcebergTableUpdate> updates =
        GlueIcebergHelper.buildIcebergTableUpdates(
            raw, TableChange.updateColumnComment(new String[] {"id"}, "primary key"));

    IcebergSchema schema = updates.get(0).schema();
    assertEquals("primary key", schema.fields().get(0).doc());
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static software.amazon.awssdk.services.glue.model.Column icebergColumn(
      String name, String type, int fieldId, boolean optional) {
    return software.amazon.awssdk.services.glue.model.Column.builder()
        .name(name)
        .type(type)
        .parameters(
            Map.of(
                GlueConstants.ICEBERG_FIELD_ID,
                String.valueOf(fieldId),
                GlueConstants.ICEBERG_FIELD_OPTIONAL,
                String.valueOf(optional)))
        .build();
  }

  private static Table icebergTable(software.amazon.awssdk.services.glue.model.Column... columns) {
    return Table.builder()
        .name(TABLE)
        .parameters(Map.of("table_type", "ICEBERG"))
        .storageDescriptor(StorageDescriptor.builder().columns(columns).build())
        .build();
  }
}
