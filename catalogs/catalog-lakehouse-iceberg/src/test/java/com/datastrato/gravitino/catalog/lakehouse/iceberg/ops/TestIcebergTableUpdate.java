/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.ops;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.ops.IcebergTableOpsHelper.IcebergTableChange;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.TableChange.ColumnPosition;
import com.datastrato.gravitino.rel.types.Types;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIcebergTableUpdate {
  private IcebergTableOps icebergTableOps = null;
  private IcebergTableOpsHelper icebergTableOpsHelper = null;
  private static final String TEST_NAMESPACE_NAME = "gravitino_test_namespace";
  private static final String TEST_TABLE_NAME = "gravitino_test_table";

  private static final TableIdentifier icebergIdentifier =
      TableIdentifier.of(TEST_NAMESPACE_NAME, TEST_TABLE_NAME);
  private static final NameIdentifier identifier =
      NameIdentifier.of(TEST_NAMESPACE_NAME, TEST_TABLE_NAME);

  private static final String[] firstField = {"foo_string"};
  private static final String[] secondField = {"foo2_string"};
  private static final String[] thirdField = {"foo_int"};
  private static final String[] fourthField = {"foo_struct"};
  private static final String[] notExistField = {"foo_not_exist"};
  private static final Schema tableSchema =
      new Schema(
          NestedField.of(1, false, firstField[0], StringType.get()),
          NestedField.of(2, false, secondField[0], StringType.get()),
          NestedField.of(3, true, thirdField[0], IntegerType.get()),
          NestedField.of(
              4,
              false,
              fourthField[0],
              ListType.ofOptional(
                  5,
                  StructType.of(
                      NestedField.required(6, "struct_int", IntegerType.get()),
                      NestedField.optional(
                          7,
                          "struct_map",
                          MapType.ofOptional(8, 9, IntegerType.get(), StringType.get()))))));

  @BeforeEach
  public void init() {
    icebergTableOps = new IcebergTableOps();
    icebergTableOpsHelper = icebergTableOps.createIcebergTableOpsHelper();
    createNamespace(TEST_NAMESPACE_NAME);
    createTable(TEST_NAMESPACE_NAME, TEST_TABLE_NAME);
  }

  public LoadTableResponse updateTable(
      NameIdentifier gravitinoNameIdentifier, TableChange... gravitinoTableChanges) {
    IcebergTableChange icebergTableChange =
        icebergTableOpsHelper.buildIcebergTableChanges(
            gravitinoNameIdentifier, gravitinoTableChanges);
    return icebergTableOps.updateTable(icebergTableChange);
  }

  private void createNamespace(String namespace) {
    icebergTableOps.createNamespace(
        CreateNamespaceRequest.builder().withNamespace(Namespace.of(namespace)).build());
  }

  private void createTable(String namespace, String tableName) {
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder().withName(tableName).withSchema(tableSchema).build();
    icebergTableOps.createTable(Namespace.of(namespace), createTableRequest);
    Assertions.assertTrue(icebergTableOps.tableExists(TableIdentifier.of(namespace, tableName)));
  }

  @Test
  public void testUpdateComment() {
    String comments = "new comment";
    TableChange updateComment = TableChange.updateComment(comments);
    LoadTableResponse loadTableResponse = updateTable(identifier, updateComment);
    String comment = loadTableResponse.tableMetadata().property("comment", "");
    Assertions.assertTrue(comment.equals(comments));
  }

  @Test
  public void testSetAndRemoveProperty() {
    String testPropertyKey = "test_property_key";
    String testPropertyValue = "test_property_value";
    String testPropertyNewValue = "test_property_new_value";
    LoadTableResponse loadTableResponse = icebergTableOps.loadTable(icebergIdentifier);
    Assertions.assertFalse(
        loadTableResponse.tableMetadata().properties().containsKey(testPropertyKey));

    // set a not-existing property
    TableChange setProperty = TableChange.setProperty(testPropertyKey, testPropertyValue);
    loadTableResponse = updateTable(identifier, setProperty);
    Assertions.assertEquals(
        loadTableResponse.tableMetadata().property(testPropertyKey, ""), testPropertyValue);

    // overwrite existing property
    setProperty = TableChange.setProperty(testPropertyKey, testPropertyNewValue);
    loadTableResponse = updateTable(identifier, setProperty);
    Assertions.assertEquals(
        loadTableResponse.tableMetadata().property(testPropertyKey, ""), testPropertyNewValue);

    // remove existing property
    TableChange removeProperty = TableChange.removeProperty(testPropertyKey);
    loadTableResponse = updateTable(identifier, removeProperty);
    Assertions.assertFalse(
        loadTableResponse.tableMetadata().properties().containsKey(testPropertyKey));

    IcebergTableOpsHelper.getIcebergReservedProperties().stream()
        .forEach(
            property -> {
              TableChange setProperty1 = TableChange.setProperty(property, "test_v");
              Assertions.assertThrowsExactly(
                  IllegalArgumentException.class, () -> updateTable(identifier, setProperty1));
            });

    IcebergTableOpsHelper.getIcebergReservedProperties().stream()
        .forEach(
            property -> {
              TableChange removeProperty1 = TableChange.removeProperty(property);
              Assertions.assertThrowsExactly(
                  IllegalArgumentException.class, () -> updateTable(identifier, removeProperty1));
            });
  }

  @Test
  public void testRenameTable() {
    TableChange renameTable = TableChange.rename("new_table_name");
    Assertions.assertThrowsExactly(
        RuntimeException.class, () -> updateTable(identifier, renameTable));
  }

  @Test
  public void testAddColumn() {
    // add to after first column
    String addColumnNameAfter = "add_column_after";
    TableChange addColumn =
        TableChange.addColumn(
            new String[] {addColumnNameAfter},
            Types.IntegerType.get(),
            "",
            ColumnPosition.after(firstField[0]));
    LoadTableResponse loadTableResponse = updateTable(identifier, addColumn);
    List<String> columns = getColumnNames(loadTableResponse);
    Assertions.assertEquals(columns.get(1), addColumnNameAfter);

    // add to first
    String addColumnNameFirst = "add_column_first";
    addColumn =
        TableChange.addColumn(
            new String[] {addColumnNameFirst}, Types.IntegerType.get(), "", ColumnPosition.first());
    loadTableResponse = updateTable(identifier, addColumn);
    columns = getColumnNames(loadTableResponse);
    Assertions.assertEquals(columns.get(0), addColumnNameFirst);

    // add to last
    String addColumnNameLast = "add_column_last";
    addColumn = TableChange.addColumn(new String[] {addColumnNameLast}, Types.IntegerType.get());
    loadTableResponse = updateTable(identifier, addColumn);
    columns = getColumnNames(loadTableResponse);
    Assertions.assertEquals(columns.get(columns.size() - 1), addColumnNameLast);

    // add to struct after
    addColumn =
        TableChange.addColumn(
            new String[] {fourthField[0], "element", "struct_after"},
            Types.IntegerType.get(),
            "",
            ColumnPosition.after("struct_int"));
    loadTableResponse = updateTable(identifier, addColumn);
    StructType t =
        (StructType)
            loadTableResponse
                .tableMetadata()
                .schema()
                .findType(IcebergTableOpsHelper.DOT.join(fourthField[0], "element"));
    Assertions.assertEquals("struct_after", t.fields().get(1).name());

    // add to struct first
    addColumn =
        TableChange.addColumn(
            new String[] {fourthField[0], "element", "struct_first"},
            Types.IntegerType.get(),
            "",
            ColumnPosition.first());
    loadTableResponse = updateTable(identifier, addColumn);
    t =
        (StructType)
            loadTableResponse
                .tableMetadata()
                .schema()
                .findType(IcebergTableOpsHelper.DOT.join(fourthField[0], "element"));
    Assertions.assertEquals("struct_first", t.fields().get(0).name());

    // add to struct last
    addColumn =
        TableChange.addColumn(
            new String[] {fourthField[0], "element", "struct_last"}, Types.IntegerType.get());
    loadTableResponse = updateTable(identifier, addColumn);
    t =
        (StructType)
            loadTableResponse
                .tableMetadata()
                .schema()
                .findType(IcebergTableOpsHelper.DOT.join(fourthField[0], "element"));
    Assertions.assertEquals("struct_last", t.fields().get(t.fields().size() - 1).name());

    // add column exists
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> {
          TableChange addColumn1 = TableChange.addColumn(firstField, Types.IntegerType.get(), "");
          updateTable(identifier, addColumn1);
        });

    // after column not exists
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> {
          TableChange addColumn1 =
              TableChange.addColumn(
                  firstField, Types.IntegerType.get(), "", ColumnPosition.after("not_exits"));
          updateTable(identifier, addColumn1);
        });

    // add required column
    IllegalArgumentException exception =
        Assertions.assertThrowsExactly(
            IllegalArgumentException.class,
            () -> {
              TableChange addColumn1 =
                  TableChange.addColumn(
                      new String[] {"required_column"}, Types.IntegerType.get(), false);
              updateTable(identifier, addColumn1);
            });
    Assertions.assertTrue(
        exception.getMessage().contains("Incompatible change: cannot add required column:"));
  }

  @Test
  public void testDeleteColumn() {
    // delete normal column
    TableChange deleteColumn = TableChange.deleteColumn(firstField, true);
    LoadTableResponse loadTableResponse = updateTable(identifier, deleteColumn);
    List<String> columns = getColumnNames(loadTableResponse);
    Assertions.assertFalse(columns.stream().anyMatch(column -> column.equals(firstField[0])));

    // delete column from list-struct
    String[] deleteColumnArray = new String[] {fourthField[0], "element", "struct_int"};
    deleteColumn = TableChange.deleteColumn(deleteColumnArray, false);
    loadTableResponse = updateTable(identifier, deleteColumn);
    Schema schema = loadTableResponse.tableMetadata().schema();
    Assertions.assertTrue(
        schema.findType(IcebergTableOpsHelper.DOT.join(deleteColumnArray)) == null);

    deleteColumn = TableChange.deleteColumn(notExistField, true);
    // no exception
    updateTable(identifier, deleteColumn);

    TableChange deleteColumn3 = TableChange.deleteColumn(notExistField, false);
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class, () -> updateTable(identifier, deleteColumn3));
  }

  @Test
  public void testUpdateColumnComment() {
    String newComment = "new comment";
    TableChange updateColumnComment = TableChange.updateColumnComment(firstField, newComment);
    LoadTableResponse loadTableResponse = updateTable(identifier, updateColumnComment);
    Assertions.assertEquals(
        loadTableResponse.tableMetadata().schema().columns().get(0).doc(), newComment);
  }

  @Test
  public void testUpdateColumnNullability() {
    TableChange updateColumnNullability = TableChange.updateColumnNullability(firstField, true);
    LoadTableResponse loadTableResponse = updateTable(identifier, updateColumnNullability);
    Assertions.assertTrue(loadTableResponse.tableMetadata().schema().columns().get(0).isOptional());

    // update struct_int from optional to required
    TableChange tableChange =
        TableChange.updateColumnNullability(
            new String[] {fourthField[0], "element", "struct_map"}, false);
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> updateTable(identifier, tableChange));
    Assertions.assertEquals(
        "Cannot change column nullability: foo_struct.element.struct_map: optional -> required",
        exception.getMessage());
  }

  @Test
  public void testUpdateColumnType() {
    TableChange updateColumnType = TableChange.updateColumnType(thirdField, Types.LongType.get());
    LoadTableResponse loadTableResponse = updateTable(identifier, updateColumnType);
    Assertions.assertEquals(
        LongType.get(), loadTableResponse.tableMetadata().schema().columns().get(2).type());

    // update struct_int from int to long
    updateColumnType =
        TableChange.updateColumnType(
            new String[] {fourthField[0], "element", "struct_int"}, Types.LongType.get());
    loadTableResponse = updateTable(identifier, updateColumnType);
    StructType t =
        (StructType)
            loadTableResponse
                .tableMetadata()
                .schema()
                .findType(IcebergTableOpsHelper.DOT.join(fourthField[0], "element"));
    Assertions.assertEquals(LongType.get(), t.fields().get(0).type());

    TableChange updateColumnType2 =
        TableChange.updateColumnType(notExistField, Types.IntegerType.get());
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class, () -> updateTable(identifier, updateColumnType2));
  }

  @Test
  public void testRenameColumn() {
    String newColumnName = "new_name";
    TableChange renameColumn = TableChange.renameColumn(firstField, newColumnName);
    LoadTableResponse loadTableResponse = updateTable(identifier, renameColumn);
    List<String> fields = getColumnNames(loadTableResponse);
    Assertions.assertEquals(newColumnName, fields.get(0));

    // rename struct_int to new_name
    renameColumn =
        TableChange.renameColumn(
            new String[] {fourthField[0], "element", "struct_int"}, newColumnName);
    loadTableResponse = updateTable(identifier, renameColumn);
    StructType t =
        (StructType)
            loadTableResponse
                .tableMetadata()
                .schema()
                .findType(IcebergTableOpsHelper.DOT.join(fourthField[0], "element"));
    Assertions.assertEquals(newColumnName, t.fields().get(0).name());

    TableChange renameColumn2 = TableChange.renameColumn(notExistField, newColumnName);
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class, () -> updateTable(identifier, renameColumn2));
  }

  private List<String> getColumnNames(LoadTableResponse loadTableResponse) {
    return loadTableResponse.tableMetadata().schema().columns().stream()
        .map(NestedField::name)
        .collect(Collectors.toList());
  }

  @Test
  public void testUpdateColumnPosition() {
    // test ColumnPosition.after
    TableChange updateColumnPosition =
        TableChange.updateColumnPosition(firstField, ColumnPosition.after(secondField[0]));
    LoadTableResponse loadTableResponse = updateTable(identifier, updateColumnPosition);
    List<String> fieldNames = getColumnNames(loadTableResponse);
    Assertions.assertEquals(fieldNames.get(0), secondField[0]);
    Assertions.assertEquals(fieldNames.get(1), firstField[0]);

    // test ColumnPosition.first
    updateColumnPosition = TableChange.updateColumnPosition(thirdField, ColumnPosition.first());
    loadTableResponse = updateTable(identifier, updateColumnPosition);
    fieldNames =
        loadTableResponse.tableMetadata().schema().columns().stream()
            .map(NestedField::name)
            .collect(Collectors.toList());
    Assertions.assertEquals(fieldNames.get(0), thirdField[0]);
    Assertions.assertEquals(fieldNames.get(1), secondField[0]);
    Assertions.assertEquals(fieldNames.get(2), firstField[0]);

    // test struct columnPosition after
    updateColumnPosition =
        TableChange.updateColumnPosition(
            new String[] {fourthField[0], "element", "struct_int"},
            ColumnPosition.after("struct_map"));
    loadTableResponse = updateTable(identifier, updateColumnPosition);
    StructType structType =
        (StructType)
            loadTableResponse
                .tableMetadata()
                .schema()
                .findType(IcebergTableOpsHelper.DOT.join(fourthField[0], "element"));
    Assertions.assertEquals("struct_map", structType.fields().get(0).name());
    Assertions.assertEquals("struct_int", structType.fields().get(1).name());

    // test struct columnPosition first
    updateColumnPosition =
        TableChange.updateColumnPosition(
            new String[] {fourthField[0], "element", "struct_int"}, ColumnPosition.first());
    loadTableResponse = updateTable(identifier, updateColumnPosition);
    structType =
        (StructType)
            loadTableResponse
                .tableMetadata()
                .schema()
                .findType(IcebergTableOpsHelper.DOT.join(fourthField[0], "element"));
    Assertions.assertEquals("struct_int", structType.fields().get(0).name());
    Assertions.assertEquals("struct_map", structType.fields().get(1).name());

    // test update first on not existing column
    TableChange updateColumnPosition2 =
        TableChange.updateColumnPosition(notExistField, ColumnPosition.first());
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class, () -> updateTable(identifier, updateColumnPosition2));

    // test update after on not existing column
    TableChange updateColumnPosition3 =
        TableChange.updateColumnPosition(thirdField, ColumnPosition.after(notExistField[0]));
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class, () -> updateTable(identifier, updateColumnPosition3));
  }

  @Test
  void testMultiUpdate() {
    // rename column
    String newColumnName = "new_name";
    TableChange renameColumn = TableChange.renameColumn(firstField, newColumnName);

    // delete column
    TableChange deleteColumn = TableChange.deleteColumn(secondField, true);

    // update properties
    String testPropertyKey = "test_property_key";
    String testPropertyValue = "test_property_value";
    TableChange setProperty = TableChange.setProperty(testPropertyKey, testPropertyValue);

    LoadTableResponse loadTableResponse =
        updateTable(identifier, renameColumn, deleteColumn, setProperty);
    List<String> columns = getColumnNames(loadTableResponse);

    Assertions.assertEquals(newColumnName, columns.get(0));

    Assertions.assertFalse(columns.stream().anyMatch(column -> column.equals(secondField[0])));

    Assertions.assertEquals(
        loadTableResponse.tableMetadata().property(testPropertyKey, ""), testPropertyValue);
  }

  @Test
  void testGetFieldName() {
    Assertions.assertEquals(null, IcebergTableOpsHelper.getParentName(new String[] {"a"}));
    Assertions.assertEquals(
        "a.b", IcebergTableOpsHelper.getParentName(new String[] {"a", "b", "c"}));

    Assertions.assertEquals("a", IcebergTableOpsHelper.getLeafName(new String[] {"a"}));
    Assertions.assertEquals("c", IcebergTableOpsHelper.getLeafName(new String[] {"a", "b", "c"}));

    Assertions.assertEquals("p", IcebergTableOpsHelper.getSiblingName(new String[] {"a"}, "p"));
    Assertions.assertEquals(
        "a.b.p", IcebergTableOpsHelper.getSiblingName(new String[] {"a", "b", "c"}, "p"));
  }
}
