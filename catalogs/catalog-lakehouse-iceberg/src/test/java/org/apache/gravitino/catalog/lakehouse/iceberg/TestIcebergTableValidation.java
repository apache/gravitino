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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;
import javax.ws.rs.core.Response;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.server.web.rest.ExceptionHandlers;
import org.apache.gravitino.server.web.rest.OperationType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestIcebergTableValidation {

  @TempDir Path warehouse;

  private IcebergBackend backend;
  private IcebergCatalogOperations operations;

  @BeforeEach
  void setUp() {
    backend =
        IcebergBackend.create(
            Map.of("catalog-backend", "memory", "warehouse", warehouse.toString()));
    operations = new IcebergCatalogOperations();
    operations.backend = backend;
    operations.createSchema(NameIdentifier.of("ml", "cat", "s"), "", Map.of());
  }

  @AfterEach
  void tearDown() {
    backend.close();
  }

  @Test
  void testDuplicateColumnsOnCreateAreBadRequest() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                create(
                    "duplicates",
                    new Column[] {
                      Column.of("id", Types.IntegerType.get()),
                      Column.of("id", Types.StringType.get())
                    }));
    assertTrue(error.getMessage().contains("multiple fields for name id"));
    assertEquals(400, tableError(OperationType.CREATE, error));
    assertFalse(operations.tableExists(table("duplicates")));
  }

  @ParameterizedTest
  @MethodSource("unparsedTypes")
  void testUnparsedTypeOnCreateIsBadRequest(Type type) {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> create("unparsed_type", new Column[] {Column.of("id", type)}));
    assertTrue(error.getMessage().contains("bigintz"));
    assertTrue(error.getMessage().contains("column 'id"));
    assertEquals(400, tableError(OperationType.CREATE, error));
    assertFalse(operations.tableExists(table("unparsed_type")));
  }

  @Test
  void testUnparsedNestedTypeReportsFullColumnPath() {
    Type type =
        Types.StructType.of(
            Types.StructType.Field.nullableField(
                "items",
                Types.ListType.nullable(
                    Types.MapType.valueNullable(
                        Types.StringType.get(),
                        Types.StructType.of(
                            Types.StructType.Field.nullableField(
                                "value", Types.UnparsedType.of("bigintz")))))));
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> create("unparsed_nested", new Column[] {Column.of("payload", type)}));
    assertEquals(
        "Unsupported type 'bigintz' for column 'payload.items.value' in an Iceberg table",
        error.getMessage());
    assertEquals(400, tableError(OperationType.CREATE, error));
    assertFalse(operations.tableExists(table("unparsed_nested")));
  }

  @ParameterizedTest
  @MethodSource("unparsedTypes")
  void testUnparsedTypeOnAddLeavesSchemaUnchanged(Type type) {
    createOrdinaryTable("unparsed_add");
    for (boolean nullable : new boolean[] {true, false}) {
      IllegalArgumentException error =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  operations.alterTable(
                      table("unparsed_add"),
                      TableChange.addColumn(new String[] {"valid"}, Types.StringType.get()),
                      TableChange.addColumn(new String[] {"bad"}, type, nullable)));
      assertTrue(error.getMessage().contains("bigintz"));
      assertEquals(400, tableError(OperationType.ALTER, error));
      assertOrdinaryColumns("unparsed_add");
    }
  }

  @Test
  void testUnparsedTypeOnUpdateLeavesSchemaUnchanged() {
    createOrdinaryTable("unparsed_update");
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                operations.alterTable(
                    table("unparsed_update"),
                    TableChange.updateColumnType(
                        new String[] {"id"}, Types.UnparsedType.of("bigintz"))));
    assertTrue(error.getMessage().contains("bigintz"));
    assertEquals(400, tableError(OperationType.ALTER, error));
    assertOrdinaryColumns("unparsed_update");
    assertEquals(
        Types.LongType.get(),
        operations.loadTable(table("unparsed_update")).columns()[0].dataType());
  }

  @Test
  void testRenameColumnConflictLeavesSchemaUnchanged() {
    createOrdinaryTable("rename_column");
    Exception error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                operations.alterTable(
                    table("rename_column"), TableChange.renameColumn(new String[] {"name"}, "id")));
    assertEquals(400, tableError(OperationType.ALTER, error));
    assertOrdinaryColumns("rename_column");
  }

  @Test
  void testDuplicateBatchAddLeavesSchemaUnchanged() {
    createOrdinaryTable("batch_add");
    Exception error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                operations.alterTable(
                    table("batch_add"),
                    TableChange.addColumn(new String[] {"batch_a"}, Types.StringType.get()),
                    TableChange.addColumn(new String[] {"batch_a"}, Types.IntegerType.get())));
    assertEquals(400, tableError(OperationType.ALTER, error));
    assertOrdinaryColumns("batch_add");
  }

  @Test
  void testAutoIncrementIsRejectedBeforeCreate() {
    Column column = Column.of("id", Types.LongType.get(), null, false, true, null);
    Exception error =
        assertThrows(
            IllegalArgumentException.class, () -> create("auto_increment", new Column[] {column}));
    assertTrue(error.getMessage().contains("auto-increment"));
    assertEquals(400, tableError(OperationType.CREATE, error));
    assertFalse(operations.tableExists(table("auto_increment")));
  }

  @ParameterizedTest
  @MethodSource("incompatiblePartitionTransforms")
  void testIncompatiblePartitionTransformIsBadRequest(Type type, Transform transform) {
    Exception error =
        assertThrows(
            IllegalArgumentException.class,
            () -> create("bad_partition", new Column[] {Column.of("value", type)}, transform));
    assertTrue(error.getMessage().contains("Invalid source type"));
    assertEquals(400, tableError(OperationType.CREATE, error));
    assertFalse(operations.tableExists(table("bad_partition")));
  }

  @Test
  void testValidCreateAndColumnChangesStillSucceed() {
    create(
        "valid", new Column[] {Column.of("value", Types.DateType.get())}, Transforms.year("value"));
    operations.alterTable(
        table("valid"),
        TableChange.renameColumn(new String[] {"value"}, "date"),
        TableChange.addColumn(new String[] {"name"}, Types.StringType.get()));
    assertArrayEquals(
        new String[] {"date", "name"},
        Arrays.stream(operations.loadTable(table("valid")).columns())
            .map(Column::name)
            .toArray(String[]::new));
  }

  @Test
  void testMultipleSingleFieldBucketTransformsArePreserved() {
    create(
        "multiple_buckets",
        new Column[] {
          Column.of("a", Types.IntegerType.get()), Column.of("b", Types.StringType.get())
        },
        Transforms.bucket(16, new String[] {"a"}),
        Transforms.bucket(8, new String[] {"b"}));
    Transform[] partitioning = operations.loadTable(table("multiple_buckets")).partitioning();
    assertEquals(2, partitioning.length);
    assertEquals(Transforms.bucket(16, new String[] {"a"}), partitioning[0]);
    assertEquals(Transforms.bucket(8, new String[] {"b"}), partitioning[1]);
  }

  @ParameterizedTest
  @MethodSource("versionThreeTypes")
  void testVersionThreeTypeWithVersionTwoIsBadRequest(Type type) {
    Exception failure =
        assertThrows(
            IllegalArgumentException.class,
            () -> create("unsupported_type", new Column[] {Column.of("value", type)}));
    assertEquals(400, tableError(OperationType.CREATE, failure));
    assertFalse(operations.tableExists(table("unsupported_type")));
  }

  @ParameterizedTest
  @MethodSource("versionThreeTypes")
  void testAddVersionThreeTypeToVersionTwoTableIsBadRequest(Type type) {
    createOrdinaryTable("add_v3_type");
    Exception failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                operations.alterTable(
                    table("add_v3_type"), TableChange.addColumn(new String[] {"value"}, type)));
    assertEquals(400, tableError(OperationType.ALTER, failure));
    assertOrdinaryColumns("add_v3_type");
  }

  @ParameterizedTest
  @MethodSource("versionThreeTypes")
  void testVersionThreeTypeRoundTrip(Type type) {
    operations.createTable(
        table("version_three"),
        new Column[] {Column.of("value", type)},
        "",
        Map.of("format-version", "3"),
        new Transform[0],
        Distributions.NONE,
        new SortOrder[0],
        new Index[0]);
    assertEquals(type, operations.loadTable(table("version_three")).columns()[0].dataType());
  }

  private static Stream<Type> unparsedTypes() {
    Type unparsed = Types.UnparsedType.of("bigintz");
    return Stream.of(
        unparsed,
        Types.ListType.nullable(unparsed),
        Types.MapType.valueNullable(unparsed, Types.StringType.get()),
        Types.MapType.valueNullable(Types.StringType.get(), unparsed),
        Types.StructType.of(Types.StructType.Field.nullableField("nested", unparsed)),
        Types.ListType.nullable(
            Types.StructType.of(Types.StructType.Field.nullableField("nested", unparsed))));
  }

  private static Stream<Type> versionThreeTypes() {
    return Stream.of(
        Types.NullType.get(),
        Types.VariantType.get(),
        Types.GeometryType.of("OGC:CRS84"),
        Types.GeographyType.of("OGC:CRS84", "spherical"));
  }

  private static Stream<Arguments> incompatiblePartitionTransforms() {
    return Stream.of(
        Arguments.of(Types.StringType.get(), Transforms.year("value")),
        Arguments.of(Types.StringType.get(), Transforms.month("value")),
        Arguments.of(Types.StringType.get(), Transforms.day("value")),
        Arguments.of(Types.StringType.get(), Transforms.hour("value")),
        Arguments.of(Types.IntegerType.get(), Transforms.day("value")),
        Arguments.of(Types.TimestampType.withoutTimeZone(), Transforms.truncate(3, "value")));
  }

  private Table create(String name, Column[] columns, Transform... partitioning) {
    return operations.createTable(
        table(name),
        columns,
        "",
        Map.of(),
        partitioning,
        Distributions.NONE,
        new SortOrder[0],
        new Index[0]);
  }

  private void createOrdinaryTable(String name) {
    create(
        name,
        new Column[] {
          Column.of("id", Types.LongType.get()), Column.of("name", Types.StringType.get())
        });
  }

  private void assertOrdinaryColumns(String name) {
    assertArrayEquals(
        new String[] {"id", "name"},
        Arrays.stream(operations.loadTable(table(name)).columns())
            .map(Column::name)
            .toArray(String[]::new));
  }

  private static NameIdentifier table(String name) {
    return NameIdentifier.of("ml", "cat", "s", name);
  }

  private static int tableError(OperationType operationType, Exception error) {
    try (Response response =
        ExceptionHandlers.handleTableException(operationType, "t", "s", error)) {
      return response.getStatus();
    }
  }
}
