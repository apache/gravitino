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

import static org.apache.gravitino.catalog.glue.GlueIcebergTableHelper.fromIcebergType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FixedType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimeType;
import org.apache.iceberg.types.Types.TimestampType;
import org.junit.jupiter.api.Test;

class TestGlueIcebergTableHelper {

  // -------------------------------------------------------------------------
  // fromIcebergType — primitive types
  // -------------------------------------------------------------------------

  @Test
  void testFromIcebergTypePrimitives() {
    assertEquals(Types.BooleanType.get(), fromIcebergType(BooleanType.get()));
    assertEquals(Types.IntegerType.get(), fromIcebergType(IntegerType.get()));
    assertEquals(Types.LongType.get(), fromIcebergType(LongType.get()));
    assertEquals(Types.FloatType.get(), fromIcebergType(FloatType.get()));
    assertEquals(Types.DoubleType.get(), fromIcebergType(DoubleType.get()));
    assertEquals(Types.StringType.get(), fromIcebergType(StringType.get()));
    assertEquals(Types.DateType.get(), fromIcebergType(DateType.get()));
  }

  @Test
  void testFromIcebergTypeTime() {
    assertEquals(Types.TimeType.of(6), fromIcebergType(TimeType.get()));
  }

  @Test
  void testFromIcebergTypeTimestamp() {
    assertEquals(
        Types.TimestampType.withoutTimeZone(6), fromIcebergType(TimestampType.withoutZone()));
    assertEquals(Types.TimestampType.withTimeZone(6), fromIcebergType(TimestampType.withZone()));
  }

  @Test
  void testFromIcebergTypeDecimal() {
    assertEquals(Types.DecimalType.of(10, 2), fromIcebergType(DecimalType.of(10, 2)));
    assertEquals(Types.DecimalType.of(38, 10), fromIcebergType(DecimalType.of(38, 10)));
  }

  @Test
  void testFromIcebergTypeFixed() {
    assertEquals(Types.FixedType.of(16), fromIcebergType(FixedType.ofLength(16)));
  }

  // -------------------------------------------------------------------------
  // fromIcebergType — nested types
  // -------------------------------------------------------------------------

  @Test
  void testFromIcebergTypeList() {
    org.apache.gravitino.rel.types.Type result =
        fromIcebergType(ListType.ofOptional(1, StringType.get()));
    assertEquals(Types.ListType.of(Types.StringType.get(), true), result);

    result = fromIcebergType(ListType.ofRequired(1, IntegerType.get()));
    assertEquals(Types.ListType.of(Types.IntegerType.get(), false), result);
  }

  @Test
  void testFromIcebergTypeMap() {
    org.apache.gravitino.rel.types.Type result =
        fromIcebergType(MapType.ofOptional(1, 2, StringType.get(), LongType.get()));
    assertEquals(Types.MapType.of(Types.StringType.get(), Types.LongType.get(), true), result);
  }

  @Test
  void testFromIcebergTypeStruct() {
    StructType icebergStruct =
        StructType.of(
            NestedField.optional(1, "name", StringType.get(), "the name"),
            NestedField.required(2, "age", IntegerType.get()));
    org.apache.gravitino.rel.types.Type result = fromIcebergType(icebergStruct);

    Types.StructType structType = (Types.StructType) result;
    assertEquals(2, structType.fields().length);
    assertEquals("name", structType.fields()[0].name());
    assertEquals(Types.StringType.get(), structType.fields()[0].type());
    assertEquals(true, structType.fields()[0].nullable());
    assertEquals("age", structType.fields()[1].name());
    assertEquals(Types.IntegerType.get(), structType.fields()[1].type());
    assertEquals(false, structType.fields()[1].nullable());
  }

  // -------------------------------------------------------------------------
  // toIcebergSchema — complex types
  // -------------------------------------------------------------------------

  @Test
  void testToIcebergSchemaWithListType() {
    Column col =
        GlueColumn.builder()
            .withName("col_list")
            .withType(Types.ListType.nullable(Types.StringType.get()))
            .build();
    Schema schema = GlueIcebergTableHelper.toIcebergSchema(new Column[] {col});

    NestedField field = schema.findField("col_list");
    assertInstanceOf(ListType.class, field.type());
    ListType listType = (ListType) field.type();
    assertEquals(StringType.get(), listType.elementType());
    assertTrue(listType.isElementOptional());
  }

  @Test
  void testToIcebergSchemaWithMapType() {
    Column col =
        GlueColumn.builder()
            .withName("col_map")
            .withType(Types.MapType.of(Types.StringType.get(), Types.LongType.get(), false))
            .build();
    Schema schema = GlueIcebergTableHelper.toIcebergSchema(new Column[] {col});

    NestedField field = schema.findField("col_map");
    assertInstanceOf(MapType.class, field.type());
    MapType mapType = (MapType) field.type();
    assertEquals(StringType.get(), mapType.keyType());
    assertEquals(LongType.get(), mapType.valueType());
    assertFalse(mapType.isValueOptional());
  }

  @Test
  void testToIcebergSchemaWithStructType() {
    Types.StructType gravitinoStruct =
        Types.StructType.of(
            Types.StructType.Field.of("name", Types.StringType.get(), true, "the name"),
            Types.StructType.Field.of("age", Types.IntegerType.get(), false, null));
    Column col = GlueColumn.builder().withName("col_struct").withType(gravitinoStruct).build();
    Schema schema = GlueIcebergTableHelper.toIcebergSchema(new Column[] {col});

    NestedField field = schema.findField("col_struct");
    assertInstanceOf(StructType.class, field.type());
    StructType structType = (StructType) field.type();
    assertEquals(2, structType.fields().size());
    assertEquals("name", structType.fields().get(0).name());
    assertEquals(StringType.get(), structType.fields().get(0).type());
    assertTrue(structType.fields().get(0).isOptional());
    assertEquals("age", structType.fields().get(1).name());
    assertEquals(IntegerType.get(), structType.fields().get(1).type());
    assertFalse(structType.fields().get(1).isOptional());
  }

  // -------------------------------------------------------------------------
  // loadTable — column type overwrite
  // -------------------------------------------------------------------------

  @Test
  void testLoadTableOverwritesColumnTypesFromIcebergSchema() {
    // Build an Iceberg schema with TIME and TIMESTAMP columns.
    Schema icebergSchema =
        new Schema(
            NestedField.optional(1, "ts", TimestampType.withZone()),
            NestedField.optional(2, "t", TimeType.get()),
            NestedField.required(3, "score", DecimalType.of(10, 2)));

    Table mockIcebergTable = mock(Table.class);
    when(mockIcebergTable.schema()).thenReturn(icebergSchema);
    when(mockIcebergTable.spec()).thenReturn(PartitionSpec.unpartitioned());
    when(mockIcebergTable.sortOrder()).thenReturn(SortOrder.unsorted());
    when(mockIcebergTable.properties()).thenReturn(new HashMap<>());

    Catalog mockCatalog = mock(Catalog.class);
    when(mockCatalog.loadTable(any(TableIdentifier.class))).thenReturn(mockIcebergTable);

    // GlueTable with wrong Hive-style column types (as Glue would store them).
    GlueColumn[] glueCols = {
      GlueColumn.builder().withName("ts").withType(Types.StringType.get()).build(),
      GlueColumn.builder().withName("t").withType(Types.StringType.get()).build(),
      GlueColumn.builder().withName("score").withType(Types.StringType.get()).build()
    };
    GlueTable table =
        GlueTable.builder()
            .withName("t1")
            .withColumns(glueCols)
            .withProperties(new HashMap<>())
            .withAuditInfo(AuditInfo.EMPTY)
            .build();

    GlueIcebergTableHelper.loadTable(mockCatalog, "db1", "t1", table);

    Column[] cols = table.columns();
    assertEquals(3, cols.length);
    assertEquals(Types.TimestampType.withTimeZone(6), cols[0].dataType());
    assertEquals(Types.TimeType.of(6), cols[1].dataType());
    assertEquals(Types.DecimalType.of(10, 2), cols[2].dataType());
  }

  @Test
  void testCreateGlueCatalog() throws IOException {
    Map<String, String> config = new HashMap<>();
    config.put(GlueConstants.AWS_REGION, "us-east-1");
    config.put(GlueConstants.WAREHOUSE, "s3://test-bucket/warehouse");

    Catalog catalog = GlueIcebergTableHelper.createGlueCatalog(config);
    GlueCatalog glueCatalog = assertInstanceOf(GlueCatalog.class, catalog);
    glueCatalog.close();
  }

  @Test
  void testBuildIcebergCatalogPropertiesWithoutStaticCredentials() {
    Map<String, String> config = new HashMap<>();
    config.put(GlueConstants.AWS_REGION, "us-east-1");
    config.put(GlueConstants.WAREHOUSE, "s3://test-bucket/warehouse");

    Map<String, String> icebergProps = GlueIcebergTableHelper.buildIcebergCatalogProperties(config);
    assertFalse(icebergProps.containsKey(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER));
    assertFalse(
        icebergProps.containsKey(
            AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER + ".access-key-id"));
    assertFalse(
        icebergProps.containsKey(
            AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER + ".secret-access-key"));
    assertFalse(icebergProps.containsKey(IcebergConstants.ICEBERG_S3_ACCESS_KEY_ID));
    assertFalse(icebergProps.containsKey(IcebergConstants.ICEBERG_S3_SECRET_ACCESS_KEY));

    config.put(GlueConstants.AWS_ACCESS_KEY_ID, "");
    config.put(GlueConstants.AWS_SECRET_ACCESS_KEY, " ");
    icebergProps = GlueIcebergTableHelper.buildIcebergCatalogProperties(config);
    assertFalse(icebergProps.containsKey(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER));
    assertFalse(
        icebergProps.containsKey(
            AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER + ".access-key-id"));
    assertFalse(
        icebergProps.containsKey(
            AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER + ".secret-access-key"));
    assertFalse(icebergProps.containsKey(IcebergConstants.ICEBERG_S3_ACCESS_KEY_ID));
    assertFalse(icebergProps.containsKey(IcebergConstants.ICEBERG_S3_SECRET_ACCESS_KEY));
  }

  @Test
  void testBuildIcebergCatalogPropertiesWithStaticCredentials() {
    Map<String, String> config = new HashMap<>();
    config.put(GlueConstants.AWS_REGION, "us-east-1");
    config.put(GlueConstants.WAREHOUSE, "s3://test-bucket/warehouse");
    config.put(GlueConstants.AWS_ACCESS_KEY_ID, "test-access-key");
    config.put(GlueConstants.AWS_SECRET_ACCESS_KEY, "test-secret-key");

    Map<String, String> icebergProps = GlueIcebergTableHelper.buildIcebergCatalogProperties(config);
    assertEquals(
        GravitinoGlueCredentialsProvider.class.getName(),
        icebergProps.get(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER));
    assertEquals(
        "test-access-key",
        icebergProps.get(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER + ".access-key-id"));
    assertEquals(
        "test-secret-key",
        icebergProps.get(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER + ".secret-access-key"));
    assertEquals("test-access-key", icebergProps.get(IcebergConstants.ICEBERG_S3_ACCESS_KEY_ID));
    assertEquals(
        "test-secret-key", icebergProps.get(IcebergConstants.ICEBERG_S3_SECRET_ACCESS_KEY));
  }

  @Test
  void testBuildIcebergCatalogPropertiesWithOnlyAccessKey() {
    Map<String, String> config = new HashMap<>();
    config.put(GlueConstants.AWS_REGION, "us-east-1");
    config.put(GlueConstants.WAREHOUSE, "s3://test-bucket/warehouse");
    config.put(GlueConstants.AWS_ACCESS_KEY_ID, "test-access-key");

    assertEquals(
        "Both 'aws-access-key-id' and 'aws-secret-access-key' must be set together. Either provide both keys for static authentication, or omit both to use the default credential chain.",
        assertThrows(
                IllegalArgumentException.class,
                () -> GlueIcebergTableHelper.buildIcebergCatalogProperties(config))
            .getMessage());
  }

  @Test
  void testBuildIcebergCatalogPropertiesWithOnlySecretKey() {
    Map<String, String> config = new HashMap<>();
    config.put(GlueConstants.AWS_REGION, "us-east-1");
    config.put(GlueConstants.WAREHOUSE, "s3://test-bucket/warehouse");
    config.put(GlueConstants.AWS_SECRET_ACCESS_KEY, "test-secret-key");

    assertEquals(
        "Both 'aws-access-key-id' and 'aws-secret-access-key' must be set together. Either provide both keys for static authentication, or omit both to use the default credential chain.",
        assertThrows(
                IllegalArgumentException.class,
                () -> GlueIcebergTableHelper.buildIcebergCatalogProperties(config))
            .getMessage());
  }

  @Test
  void testBuildIcebergCatalogPropertiesAppliesHttpTimeouts() {
    Map<String, String> config = new HashMap<>();
    config.put(GlueConstants.AWS_REGION, "us-east-1");
    config.put(GlueConstants.WAREHOUSE, "s3://test-bucket/warehouse");
    config.put(GlueConstants.AWS_GLUE_API_CALL_ATTEMPT_TIMEOUT_MS, "2500");

    Map<String, String> icebergProps = GlueIcebergTableHelper.buildIcebergCatalogProperties(config);

    assertEquals("apache", icebergProps.get("http-client.type"));
    assertEquals("2500", icebergProps.get("http-client.apache.connection-timeout-ms"));
    assertEquals("2500", icebergProps.get("http-client.apache.socket-timeout-ms"));
  }

  @Test
  void testAlterTableFindsMixedCaseColumnsByLowerCaseName() throws IOException {
    try (InMemoryCatalog catalog = new InMemoryCatalog()) {
      catalog.initialize("test", new HashMap<>());
      catalog.createNamespace(Namespace.of("db"));
      catalog.createTable(
          TableIdentifier.of("db", "events"),
          new Schema(
              NestedField.optional(1, "UserId", LongType.get()),
              NestedField.optional(2, "EventTime", TimestampType.withoutZone())));

      // Gravitino folds column names in requests to lower case before they reach the catalog.
      GlueIcebergTableHelper.alterTable(
          catalog,
          "db",
          "events",
          TableChange.deleteColumn(new String[] {"userid"}, false),
          TableChange.renameColumn(new String[] {"eventtime"}, "event_time"));

      Schema schema = catalog.loadTable(TableIdentifier.of("db", "events")).schema();
      assertEquals(1, schema.columns().size());
      assertEquals("event_time", schema.columns().get(0).name());
    }
  }

  @Test
  void testFromIcebergTypeV3Types() {
    assertEquals(
        Types.TimestampType.withTimeZone(9),
        fromIcebergType(org.apache.iceberg.types.Types.TimestampNanoType.withZone()));
    assertEquals(
        Types.TimestampType.withoutTimeZone(9),
        fromIcebergType(org.apache.iceberg.types.Types.TimestampNanoType.withoutZone()));
    assertEquals(
        Types.VariantType.get(), fromIcebergType(org.apache.iceberg.types.Types.VariantType.get()));
    assertEquals(
        Types.NullType.get(), fromIcebergType(org.apache.iceberg.types.Types.UnknownType.get()));
    assertEquals(
        Types.GeometryType.of("srid:3857"),
        fromIcebergType(org.apache.iceberg.types.Types.GeometryType.of("srid:3857")));
    assertEquals(
        Types.GeographyType.of("srid:4269", "karney"),
        fromIcebergType(
            org.apache.iceberg.types.Types.GeographyType.of(
                "srid:4269", org.apache.iceberg.types.EdgeAlgorithm.KARNEY)));
  }

  @Test
  void testToIcebergSchemaMatchesIcebergCatalogTypes() {
    Schema schema =
        GlueIcebergTableHelper.toIcebergSchema(
            new Column[] {
              column("f", Types.FixedType.of(16)),
              column("ts_ns", Types.TimestampType.withoutTimeZone(9)),
              column("v", Types.VariantType.get()),
              column("g", Types.GeometryType.of("srid:3857"))
            });
    assertEquals(FixedType.ofLength(16), schema.findType("f"));
    assertEquals(
        org.apache.iceberg.types.Types.TimestampNanoType.withoutZone(), schema.findType("ts_ns"));
    assertEquals(org.apache.iceberg.types.Types.VariantType.get(), schema.findType("v"));
    assertEquals(org.apache.iceberg.types.Types.GeometryType.of("srid:3857"), schema.findType("g"));
  }

  @Test
  void testToIcebergSchemaRefusesTypesIcebergCannotStore() {
    for (org.apache.gravitino.rel.types.Type type :
        new org.apache.gravitino.rel.types.Type[] {
          Types.ByteType.get(), Types.ShortType.get(), Types.IntegerType.unsigned()
        }) {
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class,
              () -> GlueIcebergTableHelper.toIcebergSchema(new Column[] {column("c", type)}));
      assertTrue(e.getMessage().contains(type.simpleString()), e.getMessage());
    }
  }

  @Test
  void testInvalidPartitionIsRefusedWithTheRequestedTransform() {
    Schema schema = partitionTestSchema();
    IllegalArgumentException year =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                GlueIcebergTableHelper.toPartitionSpec(
                    schema, new Transform[] {Transforms.year("name")}));
    assertTrue(year.getMessage().contains("year(name)"), year.getMessage());

    IllegalArgumentException bucket =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                GlueIcebergTableHelper.toPartitionSpec(
                    schema,
                    new Transform[] {
                      Transforms.bucket(4, new String[] {"id"}, new String[] {"name"})
                    }));
    assertTrue(bucket.getMessage().contains("bucket(4, id, name)"), bucket.getMessage());
    assertFalse(bucket.getMessage().contains("@"), bucket.getMessage());
  }

  @Test
  void testInvalidSortOrderIsRefusedWithTheRequestedTerm() {
    Schema schema = partitionTestSchema();
    IllegalArgumentException year =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                GlueIcebergTableHelper.toSortOrder(
                    schema,
                    new org.apache.gravitino.rel.expressions.sorts.SortOrder[] {
                      SortOrders.ascending(
                          FunctionExpression.of("year", NamedReference.field("name")))
                    }));
    assertTrue(year.getMessage().contains("year(name)"), year.getMessage());

    IllegalArgumentException literal =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                GlueIcebergTableHelper.toSortOrder(
                    schema,
                    new org.apache.gravitino.rel.expressions.sorts.SortOrder[] {
                      SortOrders.ascending(Literals.integerLiteral(1))
                    }));
    assertFalse(literal.getMessage().contains("LiteralImpl"), literal.getMessage());
  }

  @Test
  void testConvertPartitionSpecSkipsVoidFields() throws IOException {
    try (InMemoryCatalog catalog = newCatalog()) {
      Table table =
          catalog
              .buildTable(TableIdentifier.of("db", "t"), partitionTestSchema())
              .withPartitionSpec(
                  PartitionSpec.builderFor(partitionTestSchema())
                      .identity("name")
                      .identity("id")
                      .build())
              .withProperty("format-version", "1")
              .create();
      table.updateSpec().removeField("name").commit();
      table.refresh();

      Transform[] transforms =
          GlueIcebergTableHelper.convertPartitionSpec(table.spec(), table.schema());
      assertEquals(1, transforms.length);
      assertEquals(Transforms.identity("id"), transforms[0]);
    }
  }

  @Test
  void testAlterTableAppliesColumnPositions() throws IOException {
    try (InMemoryCatalog catalog = newCatalog()) {
      catalog.createTable(TableIdentifier.of("db", "t"), partitionTestSchema());

      GlueIcebergTableHelper.alterTable(
          catalog,
          "db",
          "t",
          TableChange.addColumn(
              new String[] {"first_col"},
              Types.IntegerType.get(),
              null,
              TableChange.ColumnPosition.first()),
          TableChange.updateColumnPosition(
              new String[] {"id"}, TableChange.ColumnPosition.after("name")));

      List<String> names =
          catalog.loadTable(TableIdentifier.of("db", "t")).schema().columns().stream()
              .map(NestedField::name)
              .collect(Collectors.toList());
      assertEquals(List.of("first_col", "name", "id"), names);
    }
  }

  @Test
  void testAlterTableIsAppliedAsAWhole() throws IOException {
    try (InMemoryCatalog catalog = newCatalog()) {
      Table table = catalog.createTable(TableIdentifier.of("db", "t"), partitionTestSchema());
      String before = ((HasTableOperations) table).operations().current().metadataFileLocation();

      // Iceberg refuses to downgrade a v2 table to v1 when the property change is staged.
      assertThrows(
          IllegalArgumentException.class,
          () ->
              GlueIcebergTableHelper.alterTable(
                  catalog,
                  "db",
                  "t",
                  TableChange.addColumn(new String[] {"partial"}, Types.StringType.get()),
                  TableChange.setProperty("format-version", "1")));
      Table reloaded = catalog.loadTable(TableIdentifier.of("db", "t"));
      assertEquals(
          before, ((HasTableOperations) reloaded).operations().current().metadataFileLocation());
      assertEquals(2, reloaded.schema().columns().size());

      assertThrows(
          IllegalArgumentException.class,
          () ->
              GlueIcebergTableHelper.alterTable(
                  catalog,
                  "db",
                  "t",
                  TableChange.updateColumnAutoIncrement(new String[] {"id"}, true)));
    }
  }

  private static InMemoryCatalog newCatalog() {
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("test", new HashMap<>());
    catalog.createNamespace(Namespace.of("db"));
    return catalog;
  }

  private static Schema partitionTestSchema() {
    return new Schema(
        NestedField.optional(1, "id", LongType.get()),
        NestedField.optional(2, "name", StringType.get()));
  }

  private static Column column(String name, org.apache.gravitino.rel.types.Type type) {
    return GlueColumn.builder().withName(name).withType(type).withNullable(true).build();
  }
}
