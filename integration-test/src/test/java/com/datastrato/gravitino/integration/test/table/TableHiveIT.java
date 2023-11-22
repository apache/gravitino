/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.exceptions.IllegalNamespaceException;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.TableChange.ColumnPosition;
import com.datastrato.gravitino.rel.TableChange.DeleteColumn;
import com.datastrato.gravitino.rel.TableChange.RemoveProperty;
import com.datastrato.gravitino.rel.TableChange.RenameColumn;
import com.datastrato.gravitino.rel.TableChange.RenameTable;
import com.datastrato.gravitino.rel.TableChange.SetProperty;
import com.datastrato.gravitino.rel.TableChange.UpdateColumnComment;
import com.datastrato.gravitino.rel.TableChange.UpdateColumnPosition;
import com.datastrato.gravitino.rel.TableChange.UpdateColumnType;
import com.datastrato.gravitino.rel.TableChange.UpdateComment;
import com.google.common.collect.Maps;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TableHiveIT extends AbstractIT {
  public static String metalakeName;
  public static String catalogName;
  public static String schemaName = "schema";
  public static String schemaComment = "schema comment";
  public static String tableName = "customers";
  private static String nameCol = "name";
  private static String dobCol = "DOB";
  private static String addressCol = "address";
  private NameIdentifier of;

  private static final String provider = "hive";

  @BeforeAll
  private static void start() {
    GravitinoITUtils.hiveConfig();
  }

  @BeforeEach
  private void before() {
    // to isolate each test in it's own space
    metalakeName = GravitinoITUtils.genRandomName("metalake");
    catalogName = GravitinoITUtils.genRandomName("catalog");

    createSchema();
  }

  @AfterEach
  private void after() {
    dropSchema();
    dropAll();
  }

  @Test
  public void testCreateTable() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "customers");
    GravitinoMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog tableCatalog = catalog.asTableCatalog();
    Table table = tableCatalog.loadTable(tableID);

    assertEquals(table.name(), tableID.name());
    assertEquals("customer table", table.comment());
    Column[] columns = table.columns();
    assertEquals(3, columns.length);
    assertEquals(nameCol, columns[0].name());
    assertEquals(TypeCreator.NULLABLE.STRING, columns[0].dataType());
    assertEquals(dobCol.toLowerCase(), columns[1].name()); // NOTE Name gets converted to lower case
    assertEquals(TypeCreator.NULLABLE.DATE, columns[1].dataType());
    assertEquals(addressCol, columns[2].name());
    assertEquals(TypeCreator.NULLABLE.STRING, columns[2].dataType());
  }

  @Test
  public void testListTables() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "customers");
    GravitinoMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog table = catalog.asTableCatalog();

    NameIdentifier[] names = table.listTables(tableID.namespace());

    assertEquals(names[0].name(), tableID.name());
  }

  @Test
  public void testTableRename() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "customers");
    GravitinoMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog table = catalog.asTableCatalog();

    NameIdentifier[] names = table.listTables(tableID.namespace());

    assertEquals(names[0].name(), tableID.name());

    String newName = GravitinoITUtils.genRandomName("users");
    RenameTable rename = (RenameTable) TableChange.rename(newName);
    table.alterTable(tableID, rename);

    names = table.listTables(tableID.namespace());
    assertEquals(names[0].name(), newName);

    table.dropTable(NameIdentifier.of(metalakeName, catalogName, schemaName, newName));
  }

  @Test
  public void testUnknownTableRename() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "sales");
    GravitinoMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog table = catalog.asTableCatalog();

    String newName = "results";
    RenameTable rename = (RenameTable) TableChange.rename(newName);

    // seems like wrong exception
    assertThrows(RuntimeException.class, () -> table.alterTable(tableID, rename));
  }

  @Test
  public void testTableCommentUpdate() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "customers");
    GravitinoMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog tableCatalog = catalog.asTableCatalog();

    String newComment = "Table of all customers";
    UpdateComment update = (UpdateComment) TableChange.updateComment(newComment);
    tableCatalog.alterTable(tableID, update);

    Table table = tableCatalog.loadTable(tableID);

    assertEquals(table.comment(), newComment);
  }

  @Test
  public void testTableInvalidCommentUpdate() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "users");
    GravitinoMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog tableCatalog = catalog.asTableCatalog();

    String newComment = "Table of all customers";
    UpdateComment update = (UpdateComment) TableChange.updateComment(newComment);
    assertThrows(RuntimeException.class, () -> tableCatalog.alterTable(tableID, update));
  }

  @Test
  public void testTableSetAndRemoveProperty() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "customers");
    GravitinoMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog tableCatalog = catalog.asTableCatalog();

    String property = "Note";
    String value = "Customers in USA only";
    SetProperty add = (SetProperty) TableChange.setProperty(property, value);

    tableCatalog.alterTable(tableID, add);

    Table table = tableCatalog.loadTable(tableID);
    Map<String, String> properties = table.properties();
    assertEquals(properties.get(property), value);

    RemoveProperty remove = (RemoveProperty) TableChange.removeProperty(property);
    tableCatalog.alterTable(tableID, remove);

    table = tableCatalog.loadTable(tableID);
    properties = table.properties();
    assertFalse(properties.containsKey(property));
  }

  @Test
  public void testAddTableColumn() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "customers");
    GravitinoMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog tableCatalog = catalog.asTableCatalog();

    String[] fields = {"newsletter"};
    TableChange column =
        TableChange.addColumn(
            fields,
            TypeCreator.NULLABLE.BOOLEAN,
            "Subscribed to newsletter?",
            ColumnPosition.after(addressCol));
    tableCatalog.alterTable(tableID, column);

    Table table = tableCatalog.loadTable(tableID);
    Column[] columns = table.columns();
    assertEquals(4, columns.length);

    ArrayList<String> names = new ArrayList<>(4);
    for (int i = 0; i < columns.length; i++) {
      names.add(columns[i].name());
    }
    assertTrue(names.contains("newsletter"));
  }

  @Test
  public void testTableRenameColumnUpdate() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "customers");
    GravitinoMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog tableCatalog = catalog.asTableCatalog();

    String[] fields = {nameCol};
    String newName = "full_name";
    RenameColumn rename = (RenameColumn) TableChange.renameColumn(fields, newName);
    tableCatalog.alterTable(tableID, rename);

    Table table = tableCatalog.loadTable(tableID);
    ArrayList<String> names = new ArrayList<>(4);
    Column[] columns = table.columns();
    for (int i = 0; i < columns.length; i++) {
      names.add(columns[i].name());
    }
    assertTrue(names.contains(newName));
    assertFalse(names.contains(nameCol));
  }

  @Test
  public void testTableColumnUpdateDatatypeNullability() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "customers");
    GravitinoMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog tableCatalog = catalog.asTableCatalog();

    String[] fields = {nameCol};
    Type newDataType = TypeCreator.REQUIRED.STRING;
    UpdateColumnType rename = (UpdateColumnType) TableChange.updateColumnType(fields, newDataType);

    tableCatalog.alterTable(tableID, rename);
  }

  @Test
  public void testTableColumnUpdateDatatype() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "customers");
    GravitinoMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog tableCatalog = catalog.asTableCatalog();

    String[] fields = {dobCol.toLowerCase()}; // NOTE name gets converted to lower case
    Type newDataType = TypeCreator.NULLABLE.STRING;
    UpdateColumnType rename = (UpdateColumnType) TableChange.updateColumnType(fields, newDataType);
    tableCatalog.alterTable(tableID, rename);

    Table table = tableCatalog.loadTable(tableID);
    Column[] columns = table.columns();
    assertEquals(dobCol.toLowerCase(), columns[1].name()); // NOTE name gets converted to lower case
    assertEquals(TypeCreator.NULLABLE.STRING, columns[1].dataType());
  }

  @Test
  public void testTableColumnUpdateComment() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "customers");
    GravitinoMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog tableCatalog = catalog.asTableCatalog();

    String[] fields = {nameCol};
    String newComment = "A customers full name.";
    UpdateColumnComment changeComment =
        (UpdateColumnComment) TableChange.updateColumnComment(fields, newComment);
    tableCatalog.alterTable(tableID, changeComment);

    Table table = tableCatalog.loadTable(tableID);
    Column[] columns = table.columns();
    assertEquals(newComment, columns[0].comment());
  }

  @Test
  public void testTableColumnUpdatePositionToLast() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "customers");
    GravitinoMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog tableCatalog = catalog.asTableCatalog();

    String[] fields = {nameCol};
    ColumnPosition position = ColumnPosition.after(addressCol);
    UpdateColumnPosition changePosition =
        (UpdateColumnPosition) TableChange.updateColumnPosition(fields, position);

    // Wrong data types
    assertThrows(
        IllegalArgumentException.class, () -> tableCatalog.alterTable(tableID, changePosition));
  }

  @Test
  public void testTableColumnUpdatePositionToFirst() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "customers");
    GravitinoMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog tableCatalog = catalog.asTableCatalog();

    String[] fields = {addressCol};
    ColumnPosition position = ColumnPosition.first();
    UpdateColumnPosition changePosition =
        (UpdateColumnPosition) TableChange.updateColumnPosition(fields, position);

    // Wrong data types
    assertThrows(
        IllegalArgumentException.class, () -> tableCatalog.alterTable(tableID, changePosition));
  }

  // Currently this test fails as Hive support for moving columns is limited and depends on their
  // datatype
  public void testTableColumnSwap() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "customers");
    GravitinoMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog tableCatalog = catalog.asTableCatalog();
    String surnameCol = "surname";

    String[] fieldsA = {surnameCol};
    TableChange column =
        TableChange.addColumn(
            fieldsA, TypeCreator.NULLABLE.STRING, "Last name", ColumnPosition.after(addressCol));
    tableCatalog.alterTable(tableID, column);

    Table table = tableCatalog.loadTable(tableID);
    Column[] columns = table.columns();
    assertEquals(nameCol.toLowerCase(), columns[0].name());
    assertEquals(dobCol.toLowerCase(), columns[1].name());
    assertEquals(addressCol.toLowerCase(), columns[2].name());
    assertEquals(surnameCol.toLowerCase(), columns[3].name());

    String[] fieldsB = {surnameCol};
    ColumnPosition position = ColumnPosition.first();
    UpdateColumnPosition changePosition =
        (UpdateColumnPosition) TableChange.updateColumnPosition(fieldsB, position);
    tableCatalog.alterTable(tableID, changePosition);

    table = tableCatalog.loadTable(tableID);
    columns = table.columns();
    assertEquals(surnameCol.toLowerCase(), columns[0].name());
    assertEquals(dobCol.toLowerCase(), columns[1].name());
    assertEquals(addressCol.toLowerCase(), columns[2].name());
    assertEquals(nameCol.toLowerCase(), columns[3].name());
  }

  @Test
  public void testTableDeleteColumn() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "customers");
    GravitinoMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog tableCatalog = catalog.asTableCatalog();

    String[] fields = {addressCol};
    DeleteColumn delete = (DeleteColumn) TableChange.deleteColumn(fields, false);
    tableCatalog.alterTable(tableID, delete);

    Table table = tableCatalog.loadTable(tableID);
    Column[] columns = table.columns();
    assertEquals(2, columns.length);
    assertEquals(nameCol.toLowerCase(), columns[0].name());
    assertEquals(dobCol.toLowerCase(), columns[1].name());
  }

  @Test
  public void testTableDeleteUnknownColumn() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "customers");
    GravitinoMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog tableCatalog = catalog.asTableCatalog();

    String[] fields = {"unknown"};
    DeleteColumn delete = (DeleteColumn) TableChange.deleteColumn(fields, false);
    assertThrows(IllegalArgumentException.class, () -> tableCatalog.alterTable(tableID, delete));
  }

  @Test
  public void testTableDeleteUnknownColumnExists() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "customers");
    GravitinoMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog tableCatalog = catalog.asTableCatalog();

    String[] fields = {"unknown"};
    DeleteColumn delete = (DeleteColumn) TableChange.deleteColumn(fields, true);
    tableCatalog.alterTable(tableID, delete);
  }

  @Test
  public void testInvalidListTables() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    GravitinoMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog table = catalog.asTableCatalog();

    assertThrows(IllegalNamespaceException.class, () -> table.listTables(schemaID.namespace()));
  }

  private static ColumnDTO[] createColumns() {
    ColumnDTO name =
        new ColumnDTO.Builder()
            .withName(nameCol)
            .withDataType(TypeCreator.NULLABLE.STRING)
            .withComment("full name")
            .build();
    ColumnDTO dob =
        new ColumnDTO.Builder()
            .withName(dobCol)
            .withDataType(TypeCreator.NULLABLE.DATE)
            .withComment("date of birth")
            .build();
    ColumnDTO address =
        new ColumnDTO.Builder()
            .withName(addressCol)
            .withDataType(TypeCreator.NULLABLE.STRING)
            .withComment("home address")
            .build();
    return new ColumnDTO[] {name, dob, address};
  }

  private static Map<String, String> createProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("description", "customers");
    return properties;
  }

  public static void createSchema() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    GravitinoMetaLake metalake =
        client.createMetalake(metalakeID, "metalake comment", Collections.emptyMap());

    Map<String, String> catalogProps = GravitinoITUtils.hiveConfigProperties();

    Catalog catalog =
        metalake.createCatalog(
            NameIdentifier.of(metalakeName, catalogName),
            Catalog.Type.RELATIONAL,
            provider,
            "sales catalog",
            catalogProps);

    Map<String, String> schemaProps = Maps.newHashMap();
    schemaProps.put("description", "sales database");
    catalog
        .asSchemas()
        .createSchema(
            NameIdentifier.of(metalakeName, catalogName, schemaName), schemaComment, schemaProps);

    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
            createColumns(),
            "customer table",
            createProperties());
  }

  public static void dropSchema() {
    GravitinoMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Catalog catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
    SupportsSchemas support = catalog.asSchemas();
    NameIdentifier[] schemas = support.listSchemas(Namespace.of(metalakeName, catalogName));

    for (NameIdentifier schema : schemas) {
      if (!schema.name().equals("default")) {
        // drops tables as well
        support.dropSchema(NameIdentifier.of(metalakeName, catalogName, schema.name()), true);
      }
    }
  }

  public static void dropAll() {
    GravitinoMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Catalog catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));

    if (metalake.catalogExists(NameIdentifier.of(metalakeName, catalogName))) {
      metalake.dropCatalog(NameIdentifier.of(metalakeName, catalogName));
    }
    client.dropMetalake(NameIdentifier.of(metalakeName));
  }
}
