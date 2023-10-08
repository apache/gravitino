/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import com.datastrato.graviton.catalog.BasePropertiesMetadata;
import com.datastrato.graviton.catalog.CatalogOperations;
import com.datastrato.graviton.catalog.PropertiesMetadata;
import com.datastrato.graviton.catalog.PropertyEntry;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchSchemaException;
import com.datastrato.graviton.exceptions.NoSuchTableException;
import com.datastrato.graviton.exceptions.NonEmptySchemaException;
import com.datastrato.graviton.exceptions.SchemaAlreadyExistsException;
import com.datastrato.graviton.exceptions.TableAlreadyExistsException;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Distribution;
import com.datastrato.graviton.rel.Schema;
import com.datastrato.graviton.rel.SchemaChange;
import com.datastrato.graviton.rel.SortOrder;
import com.datastrato.graviton.rel.SupportsSchemas;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.TableCatalog;
import com.datastrato.graviton.rel.TableChange;
import com.datastrato.graviton.rel.transforms.Transform;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class TestCatalogOperations implements CatalogOperations, TableCatalog, SupportsSchemas {

  private final Map<NameIdentifier, TestTable> tables;

  private final Map<NameIdentifier, TestSchema> schemas;

  private final BasePropertiesMetadata tablePropertiesMetadata;
  private Map<String, String> config;

  public TestCatalogOperations(Map<String, String> config) {
    tables = Maps.newHashMap();
    schemas = Maps.newHashMap();
    tablePropertiesMetadata = new TestBasePropertiesMetadata();
    this.config = config;
  }

  @Override
  public void initialize(Map<String, String> config) throws RuntimeException {}

  @Override
  public void close() throws IOException {}

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    return tables.keySet().stream()
        .filter(testTable -> testTable.namespace().equals(namespace))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    if (tables.containsKey(ident)) {
      return tables.get(ident);
    } else {
      throw new NoSuchTableException("Table " + ident + " does not exist");
    }
  }

  @Override
  public Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    AuditInfo auditInfo =
        new AuditInfo.Builder().withCreator("test").withCreateTime(Instant.now()).build();

    TestTable table =
        new TestTable.Builder()
            .withName(ident.name())
            .withComment(comment)
            .withProperties(new HashMap<>(properties))
            .withAuditInfo(auditInfo)
            .withColumns(columns)
            .withDistribution(distribution)
            .withSortOrders(sortOrders)
            .withPartitions(partitions)
            .build();

    if (tables.containsKey(ident)) {
      throw new TableAlreadyExistsException("Table " + ident + " already exists");
    } else {
      tables.put(ident, table);
    }

    return new TestTable.Builder()
        .withName(ident.name())
        .withComment(comment)
        .withProperties(new HashMap<>(properties))
        .withAuditInfo(auditInfo)
        .withColumns(columns)
        .withDistribution(distribution)
        .withSortOrders(sortOrders)
        .withPartitions(partitions)
        .build();
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    if (!tables.containsKey(ident)) {
      throw new NoSuchTableException("Table " + ident + " does not exist");
    }

    AuditInfo updatedAuditInfo =
        new AuditInfo.Builder()
            .withCreator("test")
            .withCreateTime(Instant.now())
            .withLastModifier("test")
            .withLastModifiedTime(Instant.now())
            .build();

    TestTable table = tables.get(ident);
    Map<String, String> newProps =
        table.properties() != null ? Maps.newHashMap(table.properties()) : Maps.newHashMap();

    for (TableChange change : changes) {
      if (change instanceof TableChange.SetProperty) {
        newProps.put(
            ((TableChange.SetProperty) change).getProperty(),
            ((TableChange.SetProperty) change).getValue());
      } else if (change instanceof TableChange.RemoveProperty) {
        newProps.remove(((TableChange.RemoveProperty) change).getProperty());
      } else {
        throw new IllegalArgumentException("Unsupported table change: " + change);
      }
    }

    TestTable updatedTable =
        new TestTable.Builder()
            .withName(ident.name())
            .withComment(table.comment())
            .withProperties(new HashMap<>(newProps))
            .withAuditInfo(updatedAuditInfo)
            .withColumns(table.columns())
            .withPartitions(table.partitioning())
            .build();

    tables.put(ident, updatedTable);
    return new TestTable.Builder()
        .withName(ident.name())
        .withComment(table.comment())
        .withProperties(new HashMap<>(newProps))
        .withAuditInfo(updatedAuditInfo)
        .withColumns(table.columns())
        .withPartitions(table.partitioning())
        .build();
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    if (tables.containsKey(ident)) {
      tables.remove(ident);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    return schemas.keySet().stream()
        .filter(ident -> ident.namespace().equals(namespace))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> metadata)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    AuditInfo auditInfo =
        new AuditInfo.Builder().withCreator("test").withCreateTime(Instant.now()).build();

    TestSchema schema =
        new TestSchema.Builder()
            .withName(ident.name())
            .withComment(comment)
            .withProperties(metadata)
            .withAuditInfo(auditInfo)
            .build();

    if (schemas.containsKey(ident)) {
      throw new SchemaAlreadyExistsException("Schema " + ident + " already exists");
    } else {
      schemas.put(ident, schema);
    }

    return schema;
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    if (schemas.containsKey(ident)) {
      return schemas.get(ident);
    } else {
      throw new NoSuchSchemaException("Schema " + ident + " does not exist");
    }
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    if (!schemas.containsKey(ident)) {
      throw new NoSuchSchemaException("Schema " + ident + " does not exist");
    }

    AuditInfo updatedAuditInfo =
        new AuditInfo.Builder()
            .withCreator("test")
            .withCreateTime(Instant.now())
            .withLastModifier("test")
            .withLastModifiedTime(Instant.now())
            .build();

    TestSchema schema = schemas.get(ident);
    Map<String, String> newProps =
        schema.properties() != null ? Maps.newHashMap(schema.properties()) : Maps.newHashMap();

    for (SchemaChange change : changes) {
      if (change instanceof SchemaChange.SetProperty) {
        newProps.put(
            ((SchemaChange.SetProperty) change).getProperty(),
            ((SchemaChange.SetProperty) change).getValue());
      } else if (change instanceof SchemaChange.RemoveProperty) {
        newProps.remove(((SchemaChange.RemoveProperty) change).getProperty());
      } else {
        throw new IllegalArgumentException("Unsupported schema change: " + change);
      }
    }

    TestSchema updatedSchema =
        new TestSchema.Builder()
            .withName(ident.name())
            .withComment(schema.comment())
            .withProperties(newProps)
            .withAuditInfo(updatedAuditInfo)
            .build();

    schemas.put(ident, updatedSchema);
    return updatedSchema;
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    if (!schemas.containsKey(ident)) {
      return false;
    }

    schemas.remove(ident);
    if (cascade) {
      tables.keySet().stream()
          .filter(table -> table.namespace().toString().equals(ident.toString()))
          .forEach(tables::remove);
    }

    return true;
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    return tablePropertiesMetadata;
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    if (config.containsKey("mock")) {
      return new BasePropertiesMetadata() {
        @Override
        protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
          return ImmutableMap.<String, PropertyEntry<?>>builder()
              .put(
                  "key1",
                  PropertyEntry.stringPropertyEntry(
                      "key1", "value1", true, true, null, false, false))
              .put(
                  "key2",
                  PropertyEntry.stringPropertyEntry(
                      "key2", "value2", true, false, null, false, false))
              .put(
                  "key3",
                  new PropertyEntry.Builder<Integer>()
                      .withDecoder(Integer::parseInt)
                      .withEncoder(Object::toString)
                      .withDefaultValue(1)
                      .withDescription("key3")
                      .withHidden(false)
                      .withReserved(false)
                      .withImmutable(true)
                      .withJavaType(Integer.class)
                      .withRequired(false)
                      .withName("key3")
                      .build())
              .put(
                  "key4",
                  PropertyEntry.stringPropertyEntry(
                      "key4", "value4", false, false, "value4", false, false))
              .put(
                  "reserved_key",
                  PropertyEntry.stringPropertyEntry(
                      "reserved_key", "reserved_key", false, true, "reserved_value", false, true))
              .put(
                  "hidden_key",
                  PropertyEntry.stringPropertyEntry(
                      "hidden_key", "hidden_key", false, false, "hidden_value", true, false))
              .build();
        }
      };
    }
    return Maps::newHashMap;
  }
}
