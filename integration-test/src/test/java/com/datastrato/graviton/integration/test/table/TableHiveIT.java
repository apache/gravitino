/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.test.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.client.GravitonMetaLake;
import com.datastrato.graviton.dto.rel.ColumnDTO;
import com.datastrato.graviton.exceptions.IllegalNamespaceException;
import com.datastrato.graviton.integration.test.util.AbstractIT;
import com.datastrato.graviton.integration.test.util.GravitonITUtils;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.SupportsSchemas;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.TableCatalog;
import com.datastrato.graviton.rel.TableChange;
import com.datastrato.graviton.rel.TableChange.ColumnPosition;
import com.datastrato.graviton.rel.TableChange.DeleteColumn;
import com.datastrato.graviton.rel.TableChange.RemoveProperty;
import com.datastrato.graviton.rel.TableChange.RenameColumn;
import com.datastrato.graviton.rel.TableChange.RenameTable;
import com.datastrato.graviton.rel.TableChange.SetProperty;
import com.datastrato.graviton.rel.TableChange.UpdateColumnComment;
import com.datastrato.graviton.rel.TableChange.UpdateColumnPosition;
import com.datastrato.graviton.rel.TableChange.UpdateColumnType;
import com.datastrato.graviton.rel.TableChange.UpdateComment;
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

  @BeforeAll
  private static void start() {
    GravitonITUtils.hiveConfig();
  }

  @BeforeEach
  private void before() {
    // to isolate each test in it's own space
    metalakeName = GravitonITUtils.genRandomName("metalake");
    catalogName = GravitonITUtils.genRandomName("catalog");

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
    GravitonMetaLake metalake = client.loadMetalake(metalakeID);
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
    GravitonMetaLake metalake = client.loadMetalake(metalakeID);
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
    GravitonMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog table = catalog.asTableCatalog();

    NameIdentifier[] names = table.listTables(tableID.namespace());

    assertEquals(names[0].name(), tableID.name());

    String newName = "users";
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
    GravitonMetaLake metalake = client.loadMetalake(metalakeID);
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
    GravitonMetaLake metalake = client.loadMetalake(metalakeID);
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
    GravitonMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog tableCatalog = catalog.asTableCatalog();

    String newComment = "Table of all customers";
    UpdateComment update = (UpdateComment) TableChange.updateComment(newComment);
    assertThrows(RuntimeException.class, () -> tableCatalog.alterTable(tableID, update));
  }

  public void testTableSetAndRemoveProperty() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "customers");
    GravitonMetaLake metalake = client.loadMetalake(metalakeID);
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
    GravitonMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog tableCatalog = catalog.asTableCatalog();

    String[] fields = {"newsletter"};
    TableChange column =
        TableChange.addColumn(fields, TypeCreator.NULLABLE.BOOLEAN, "Subscribed to newsletter?");
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
    GravitonMetaLake metalake = client.loadMetalake(metalakeID);
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
    GravitonMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog tableCatalog = catalog.asTableCatalog();

    String[] fields = {nameCol};
    Type newDataType = TypeCreator.REQUIRED.STRING;
    UpdateColumnType rename = (UpdateColumnType) TableChange.updateColumnType(fields, newDataType);
    tableCatalog.alterTable(tableID, rename);

    Table table = tableCatalog.loadTable(tableID);
    Column[] columns = table.columns();
    assertEquals(nameCol, columns[0].name());
    assertEquals(TypeCreator.REQUIRED.STRING, columns[0].dataType());
  }

  @Test
  public void testTableColumnUpdateDatatype() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "customers");
    GravitonMetaLake metalake = client.loadMetalake(metalakeID);
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
    GravitonMetaLake metalake = client.loadMetalake(metalakeID);
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
    GravitonMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog tableCatalog = catalog.asTableCatalog();

    String[] fields = {nameCol};
    ColumnPosition position = ColumnPosition.after(addressCol);
    UpdateColumnPosition changePosition =
        (UpdateColumnPosition) TableChange.updateColumnPosition(fields, position);
    tableCatalog.alterTable(tableID, changePosition);

    Table table = tableCatalog.loadTable(tableID);
    Column[] columns = table.columns();
    assertEquals(dobCol.toLowerCase(), columns[0].name());
    assertEquals(addressCol.toLowerCase(), columns[1].name());
    assertEquals(nameCol.toLowerCase(), columns[2].name());
  }

  @Test
  public void testTableColumnUpdatePositionToFirst() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "customers");
    GravitonMetaLake metalake = client.loadMetalake(metalakeID);
    Catalog catalog = metalake.loadCatalog(catalogID);
    catalog.asSchemas().loadSchema(schemaID);
    TableCatalog tableCatalog = catalog.asTableCatalog();

    String[] fields = {addressCol};
    ColumnPosition position = ColumnPosition.first();
    UpdateColumnPosition changePosition =
        (UpdateColumnPosition) TableChange.updateColumnPosition(fields, position);
    tableCatalog.alterTable(tableID, changePosition);

    Table table = tableCatalog.loadTable(tableID);
    Column[] columns = table.columns();
    assertEquals(addressCol.toLowerCase(), columns[0].name());
    assertEquals(nameCol.toLowerCase(), columns[1].name());
    assertEquals(dobCol.toLowerCase(), columns[2].name());
  }

  @Test
  public void testTableDeleteColumn() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    NameIdentifier catalogID = NameIdentifier.of(metalakeName, catalogName);
    NameIdentifier schemaID = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NameIdentifier tableID = NameIdentifier.of(metalakeName, catalogName, schemaName, "customers");
    GravitonMetaLake metalake = client.loadMetalake(metalakeID);
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
    GravitonMetaLake metalake = client.loadMetalake(metalakeID);
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
    GravitonMetaLake metalake = client.loadMetalake(metalakeID);
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
    GravitonMetaLake metalake = client.loadMetalake(metalakeID);
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
    GravitonMetaLake metalake =
        client.createMetalake(metalakeID, "metalake comment", Collections.emptyMap());

    Map<String, String> catalogProps = GravitonITUtils.hiveConfigProperties();

    Catalog catalog =
        metalake.createCatalog(
            NameIdentifier.of(metalakeName, catalogName),
            Catalog.Type.RELATIONAL,
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
    GravitonMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
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
    GravitonMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Catalog catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));

    if (metalake.catalogExists(NameIdentifier.of(metalakeName, catalogName))) {
      metalake.dropCatalog(NameIdentifier.of(metalakeName, catalogName));
    }
    client.dropMetalake(NameIdentifier.of(metalakeName));
  }
}
