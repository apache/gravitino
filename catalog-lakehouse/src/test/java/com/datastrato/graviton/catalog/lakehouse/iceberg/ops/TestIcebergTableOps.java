package com.datastrato.graviton.catalog.lakehouse.iceberg.ops;

import com.datastrato.graviton.rel.TableChange;
import com.datastrato.graviton.rel.TableChange.ColumnPosition;
import io.substrait.type.Type.I32;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIcebergTableOps {
  private IcebergTableOps icebergTableOps = null;
  private static final String TEST_NAMESPACE_NAME = "graviton_test_namespace";
  private static final String TEST_TABLE_NAME = "graviton_test_table";

  private static final TableIdentifier identifier =
      TableIdentifier.of(TEST_NAMESPACE_NAME, TEST_TABLE_NAME);

  private static final String[] firstField = {"foo_string"};
  private static final String[] secondField = {"foo2_string"};
  private static final String[] thirdField = {"foo_int"};
  private static final String[] notExistField = {"foo_not_exist"};
  private static final Schema tableSchema =
      new Schema(
          NestedField.of(1, false, firstField[0], StringType.get()),
          NestedField.of(2, false, secondField[0], StringType.get()),
          NestedField.of(3, false, thirdField[0], IntegerType.get()));

  @BeforeEach
  public void init() {
    icebergTableOps = new IcebergTableOps();
    createNamespace(TEST_NAMESPACE_NAME);
    createTable(TEST_NAMESPACE_NAME, TEST_TABLE_NAME);
  }

  @AfterEach
  public void close() {
    System.out.flush();
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
    LoadTableResponse loadTableResponse = icebergTableOps.alterTable(identifier, updateComment);
    String comment = loadTableResponse.tableMetadata().property("comment", "");
    Assertions.assertTrue(comment.equals(comments));
  }

  @Test
  public void testSetAndRemoveProperty() {
    String testPropertyKey = "test_property_key";
    String testPropertyValue = "test_property_value";
    String testPropertyNewValue = "test_property_new_value";
    LoadTableResponse loadTableResponse = icebergTableOps.loadTable(identifier);
    Assertions.assertFalse(
        loadTableResponse.tableMetadata().properties().containsKey(testPropertyKey));

    // set a not-existing property
    TableChange setProperty = TableChange.setProperty(testPropertyKey, testPropertyValue);
    loadTableResponse = icebergTableOps.alterTable(identifier, setProperty);
    Assertions.assertEquals(
        loadTableResponse.tableMetadata().property(testPropertyKey, ""), testPropertyValue);

    // overwrite existing property
    setProperty = TableChange.setProperty(testPropertyKey, testPropertyNewValue);
    loadTableResponse = icebergTableOps.alterTable(identifier, setProperty);
    Assertions.assertEquals(
        loadTableResponse.tableMetadata().property(testPropertyKey, ""), testPropertyNewValue);

    // remove existing property
    TableChange removeProperty = TableChange.removeProperty(testPropertyKey);
    loadTableResponse = icebergTableOps.alterTable(identifier, removeProperty);
    Assertions.assertFalse(
        loadTableResponse.tableMetadata().properties().containsKey(testPropertyKey));

    icebergTableOps.getIcebergBanedProperties().stream()
        .forEach(
            property -> {
              TableChange setProperty1 = TableChange.setProperty(property, "test_v");
              Assertions.assertThrowsExactly(
                  IllegalArgumentException.class,
                  () -> icebergTableOps.alterTable(identifier, setProperty1));
            });

    icebergTableOps.getIcebergBanedProperties().stream()
        .forEach(
            property -> {
              TableChange removeProperty1 = TableChange.removeProperty(property);
              Assertions.assertThrowsExactly(
                  IllegalArgumentException.class,
                  () -> icebergTableOps.alterTable(identifier, removeProperty1));
            });
  }

  @Test
  public void testRenameProperty() {
    String newTableName = "new_table_name";
    TableChange renameTable = TableChange.rename("new_table_name");
    icebergTableOps.alterTable(identifier, renameTable);
    Assertions.assertFalse(icebergTableOps.tableExists(identifier));
    Assertions.assertTrue(
        icebergTableOps.tableExists(TableIdentifier.of(TEST_NAMESPACE_NAME, newTableName)));
  }

  @Test
  public void testAddColumn() {
    // add to after first column
    String addColumnNameAfter = "add_column_after";
    TableChange addColumn3 =
        TableChange.addColumn(
            new String[] {addColumnNameAfter},
            I32.builder().nullable(true).build(),
            "",
            ColumnPosition.after(firstField[0]));
    LoadTableResponse loadTableResponse = icebergTableOps.alterTable(identifier, addColumn3);
    List<String> columns = getColumnNames(loadTableResponse);
    Assertions.assertEquals(columns.get(1), addColumnNameAfter);

    // add to first
    String addColumnNameFirst = "add_column_first";
    TableChange addColumn =
        TableChange.addColumn(
            new String[] {addColumnNameFirst},
            I32.builder().nullable(true).build(),
            "",
            ColumnPosition.first());
    loadTableResponse = icebergTableOps.alterTable(identifier, addColumn);
    columns = getColumnNames(loadTableResponse);
    Assertions.assertEquals(columns.get(0), addColumnNameFirst);

    // add to last
    String addColumnNameLast = "add_column_last";
    TableChange addColumn2 =
        TableChange.addColumn(
            new String[] {addColumnNameLast}, I32.builder().nullable(true).build());
    loadTableResponse = icebergTableOps.alterTable(identifier, addColumn2);
    columns = getColumnNames(loadTableResponse);
    Assertions.assertEquals(columns.get(columns.size() - 1), addColumnNameLast);
  }

  @Test
  public void testDeleteColumn() {
    TableChange deleteColumn = TableChange.deleteColumn(firstField, true);
    LoadTableResponse loadTableResponse = icebergTableOps.alterTable(identifier, deleteColumn);
    List<String> columns = getColumnNames(loadTableResponse);
    Assertions.assertFalse(columns.stream().anyMatch(column -> column.equals(firstField[0])));

    deleteColumn = TableChange.deleteColumn(notExistField, true);
    // no exception
    icebergTableOps.alterTable(identifier, deleteColumn);

    TableChange deleteColumn3 = TableChange.deleteColumn(notExistField, false);
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> icebergTableOps.alterTable(identifier, deleteColumn3));
  }

  @Test
  public void testUpdateColumnComment() {
    String newComment = "new comment";
    TableChange updateColumnComment = TableChange.updateColumnComment(firstField, newComment);
    LoadTableResponse loadTableResponse =
        icebergTableOps.alterTable(identifier, updateColumnComment);
    Assertions.assertEquals(
        loadTableResponse.tableMetadata().schema().columns().get(0).doc(), newComment);
  }

  @Test
  public void testUpdateColumnType() {
    TableChange updateColumnType =
        TableChange.updateColumnType(firstField, I32.builder().nullable(true).build());
    LoadTableResponse loadTableResponse = icebergTableOps.alterTable(identifier, updateColumnType);
    Assertions.assertEquals(
        IntegerType.get(), loadTableResponse.tableMetadata().schema().columns().get(0).type());

    TableChange updateColumnType2 =
        TableChange.updateColumnType(notExistField, I32.builder().nullable(true).build());
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> icebergTableOps.alterTable(identifier, updateColumnType2));
  }

  @Test
  public void testRenameColumn() {
    String newColumnName = "new_name";
    TableChange renameColumn = TableChange.renameColumn(firstField, newColumnName);
    LoadTableResponse loadTableResponse = icebergTableOps.alterTable(identifier, renameColumn);
    List<String> fields = getColumnNames(loadTableResponse);
    Assertions.assertEquals(newColumnName, fields.get(0));

    TableChange renameColumn2 = TableChange.renameColumn(notExistField, newColumnName);
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> icebergTableOps.alterTable(identifier, renameColumn2));
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
    LoadTableResponse loadTableResponse =
        icebergTableOps.alterTable(identifier, updateColumnPosition);
    List<String> fieldNames = getColumnNames(loadTableResponse);
    Assertions.assertEquals(3, fieldNames.size());
    Assertions.assertEquals(fieldNames.get(0), secondField[0]);
    Assertions.assertEquals(fieldNames.get(1), firstField[0]);

    // test ColumnPosition.first
    updateColumnPosition = TableChange.updateColumnPosition(thirdField, ColumnPosition.first());
    loadTableResponse = icebergTableOps.alterTable(identifier, updateColumnPosition);
    fieldNames =
        loadTableResponse.tableMetadata().schema().columns().stream()
            .map(NestedField::name)
            .collect(Collectors.toList());
    Assertions.assertEquals(3, fieldNames.size());
    Assertions.assertEquals(fieldNames.get(0), thirdField[0]);
    Assertions.assertEquals(fieldNames.get(1), secondField[0]);
    Assertions.assertEquals(fieldNames.get(2), firstField[0]);

    // test update a not existing column
    TableChange updateColumnPosition2 =
        TableChange.updateColumnPosition(notExistField, ColumnPosition.first());
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> icebergTableOps.alterTable(identifier, updateColumnPosition2));

    // test update after a not existing column
    TableChange updateColumnPosition3 =
        TableChange.updateColumnPosition(notExistField, ColumnPosition.after(notExistField[0]));
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> icebergTableOps.alterTable(identifier, updateColumnPosition2));
  }
}
