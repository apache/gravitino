/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg;

import static com.datastrato.graviton.rel.transforms.Transforms.bucket;
import static com.datastrato.graviton.rel.transforms.Transforms.day;
import static com.datastrato.graviton.rel.transforms.Transforms.identity;
import static com.datastrato.graviton.rel.transforms.Transforms.truncate;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.exceptions.NoSuchSchemaException;
import com.datastrato.graviton.exceptions.TableAlreadyExistsException;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.CatalogEntity;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Distribution;
import com.datastrato.graviton.rel.SortOrder;
import com.datastrato.graviton.rel.SortOrder.Direction;
import com.datastrato.graviton.rel.SortOrder.NullOrdering;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.TableCatalog;
import com.datastrato.graviton.rel.TableChange;
import com.datastrato.graviton.rel.transforms.Transform;
import com.datastrato.graviton.rel.transforms.Transforms;
import com.google.common.collect.Maps;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestIcebergTable {

  private static final String META_LAKE_NAME = "metalake";

  private static final String ICEBERG_CATALOG_NAME = "test_catalog";
  private static final String ICEBERG_SCHEMA_NAME = "test_schema";
  private static final String ICEBERG_COMMENT = "test_comment";
  private static IcebergCatalog icebergCatalog;
  private static IcebergSchema icebergSchema;
  private static final NameIdentifier schemaIdent =
      NameIdentifier.of(META_LAKE_NAME, ICEBERG_CATALOG_NAME, ICEBERG_SCHEMA_NAME);

  @BeforeAll
  private static void setup() {
    initIcebergCatalog();
    initIcebergSchema();
  }

  @AfterEach
  private void resetSchema() {
    TableCatalog tableCatalog = icebergCatalog.asTableCatalog();
    NameIdentifier[] nameIdentifiers =
        tableCatalog.listTables(
            Namespace.of(ArrayUtils.add(schemaIdent.namespace().levels(), schemaIdent.name())));
    if (ArrayUtils.isNotEmpty(nameIdentifiers)) {
      Arrays.stream(nameIdentifiers).forEach(tableCatalog::dropTable);
    }
    icebergCatalog.asSchemas().dropSchema(schemaIdent, false);
    initIcebergSchema();
  }

  private static void initIcebergSchema() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    icebergSchema =
        (IcebergSchema)
            icebergCatalog.asSchemas().createSchema(schemaIdent, ICEBERG_COMMENT, properties);
  }

  private static void initIcebergCatalog() {
    CatalogEntity entity = createDefaultCatalogEntity();

    Map<String, String> conf = Maps.newHashMap();
    icebergCatalog = new IcebergCatalog().withCatalogConf(conf).withCatalogEntity(entity);
  }

  private static CatalogEntity createDefaultCatalogEntity() {
    AuditInfo auditInfo =
        new AuditInfo.Builder()
            .withCreator("testIcebergUser")
            .withCreateTime(Instant.now())
            .build();

    CatalogEntity entity =
        new CatalogEntity.Builder()
            .withId(1L)
            .withName(ICEBERG_CATALOG_NAME)
            .withNamespace(Namespace.of(META_LAKE_NAME))
            .withType(IcebergCatalog.Type.RELATIONAL)
            .withProvider("iceberg")
            .withAuditInfo(auditInfo)
            .build();
    return entity;
  }

  private SortOrder[] createSortOrder() {
    return new SortOrder[] {
      SortOrder.builder()
          .withNullOrdering(NullOrdering.FIRST)
          .withDirection(Direction.DESC)
          .withTransform(Transforms.field(new String[] {"col_2"}))
          .build()
    };
  }

  @Test
  public void testCreateIcebergTable() throws IOException {
    String icebergTableName = "test_iceberg_table";
    NameIdentifier tableIdentifier =
        NameIdentifier.of(
            META_LAKE_NAME, icebergCatalog.name(), icebergSchema.name(), icebergTableName);
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    IcebergColumn col1 =
        new IcebergColumn.Builder()
            .withName("col_1")
            .withType(TypeCreator.NULLABLE.I8)
            .withComment(ICEBERG_COMMENT)
            .build();
    IcebergColumn col2 =
        new IcebergColumn.Builder()
            .withName("col_2")
            .withType(TypeCreator.NULLABLE.DATE)
            .withComment(ICEBERG_COMMENT)
            .build();
    Column[] columns = new Column[] {col1, col2};

    SortOrder[] sortOrders = createSortOrder();
    Table table =
        icebergCatalog
            .asTableCatalog()
            .createTable(
                tableIdentifier,
                columns,
                ICEBERG_COMMENT,
                properties,
                new Transform[0],
                Distribution.NONE,
                sortOrders);
    Assertions.assertEquals(tableIdentifier.name(), table.name());
    Assertions.assertEquals(ICEBERG_COMMENT, table.comment());
    Assertions.assertEquals("val1", table.properties().get("key1"));
    Assertions.assertEquals("val2", table.properties().get("key2"));

    Table loadedTable = icebergCatalog.asTableCatalog().loadTable(tableIdentifier);

    Assertions.assertEquals("val1", loadedTable.properties().get("key1"));
    Assertions.assertEquals("val2", loadedTable.properties().get("key2"));

    Assertions.assertTrue(icebergCatalog.asTableCatalog().tableExists(tableIdentifier));
    NameIdentifier[] tableIdents =
        icebergCatalog.asTableCatalog().listTables(tableIdentifier.namespace());
    Assertions.assertTrue(Arrays.asList(tableIdents).contains(tableIdentifier));

    Assertions.assertEquals(sortOrders.length, loadedTable.sortOrder().length);
    for (int i = 0; i < loadedTable.sortOrder().length; i++) {
      Assertions.assertEquals(
          sortOrders[i].getDirection(), loadedTable.sortOrder()[i].getDirection());
      Assertions.assertEquals(
          sortOrders[i].getTransform(), loadedTable.sortOrder()[i].getTransform());
    }
    // Compare sort and order

    // Test exception
    Throwable exception =
        Assertions.assertThrows(
            TableAlreadyExistsException.class,
            () ->
                icebergCatalog
                    .asTableCatalog()
                    .createTable(
                        tableIdentifier,
                        columns,
                        ICEBERG_COMMENT,
                        properties,
                        new Transform[0],
                        Distribution.NONE,
                        sortOrders));
    Assertions.assertTrue(exception.getMessage().contains("Table already exists"));
  }

  @Test
  public void testCreatePartitionedIcebergTable() {
    NameIdentifier tableIdentifier =
        NameIdentifier.of(
            META_LAKE_NAME, icebergCatalog.name(), icebergSchema.name(), genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    IcebergColumn col1 =
        new IcebergColumn.Builder()
            .withName("city")
            .withType(TypeCreator.NULLABLE.I8)
            .withComment(ICEBERG_COMMENT)
            .build();
    IcebergColumn col2 =
        new IcebergColumn.Builder()
            .withName("date")
            .withType(TypeCreator.NULLABLE.DATE)
            .withComment(ICEBERG_COMMENT)
            .build();
    Column[] columns = new Column[] {col1, col2};

    Transform[] partitions =
        new Transform[] {
          day(new String[] {col2.name()}),
          bucket(new String[] {col1.name()}, 10),
          truncate(new String[] {col1.name()}, 2)
        };

    Table table =
        icebergCatalog
            .asTableCatalog()
            .createTable(tableIdentifier, columns, ICEBERG_COMMENT, properties, partitions);
    Assertions.assertEquals(tableIdentifier.name(), table.name());
    Assertions.assertEquals(ICEBERG_COMMENT, table.comment());
    Assertions.assertEquals("val1", table.properties().get("key1"));
    Assertions.assertEquals("val2", table.properties().get("key2"));
    Assertions.assertArrayEquals(partitions, table.partitioning());

    Table loadedTable = icebergCatalog.asTableCatalog().loadTable(tableIdentifier);

    Assertions.assertEquals("val1", loadedTable.properties().get("key1"));
    Assertions.assertEquals("val2", loadedTable.properties().get("key2"));
    Assertions.assertArrayEquals(partitions, loadedTable.partitioning());

    Assertions.assertTrue(icebergCatalog.asTableCatalog().tableExists(tableIdentifier));
    NameIdentifier[] tableIdents =
        icebergCatalog.asTableCatalog().listTables(tableIdentifier.namespace());
    Assertions.assertTrue(Arrays.asList(tableIdents).contains(tableIdentifier));

    // Test exception
    Throwable exception =
        Assertions.assertThrows(
            TableAlreadyExistsException.class,
            () ->
                icebergCatalog
                    .asTableCatalog()
                    .createTable(
                        tableIdentifier,
                        columns,
                        ICEBERG_COMMENT,
                        properties,
                        new Transform[] {day(new String[] {col2.name()})}));
    Assertions.assertTrue(exception.getMessage().contains("Table already exists"));

    exception =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                icebergCatalog
                    .asTableCatalog()
                    .createTable(
                        NameIdentifier.of(
                            META_LAKE_NAME,
                            icebergCatalog.name(),
                            icebergSchema.name(),
                            genRandomName()),
                        columns,
                        ICEBERG_COMMENT,
                        properties,
                        new Transform[] {identity(new String[] {col1.name(), col2.name()})}));
    Assertions.assertTrue(
        exception.getMessage().contains("Iceberg partition does not support nested field"));

    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                icebergCatalog
                    .asTableCatalog()
                    .createTable(
                        NameIdentifier.of(
                            META_LAKE_NAME,
                            icebergCatalog.name(),
                            icebergSchema.name(),
                            genRandomName()),
                        columns,
                        ICEBERG_COMMENT,
                        properties,
                        new Transform[] {identity(new String[] {"not_exist_field"})}));
    Assertions.assertTrue(
        exception.getMessage().contains("Cannot find source column: not_exist_field"));
  }

  @Test
  public void testDropIcebergTable() {
    NameIdentifier tableIdentifier =
        NameIdentifier.of(
            META_LAKE_NAME, icebergCatalog.name(), icebergSchema.name(), genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    IcebergColumn col1 =
        new IcebergColumn.Builder()
            .withName("col_1")
            .withType(TypeCreator.NULLABLE.I8)
            .withComment(ICEBERG_COMMENT)
            .build();
    IcebergColumn col2 =
        new IcebergColumn.Builder()
            .withName("col_2")
            .withType(TypeCreator.NULLABLE.DATE)
            .withComment(ICEBERG_COMMENT)
            .build();
    Column[] columns = new Column[] {col1, col2};

    icebergCatalog
        .asTableCatalog()
        .createTable(
            tableIdentifier,
            columns,
            ICEBERG_COMMENT,
            properties,
            new Transform[0],
            Distribution.NONE,
            new SortOrder[0]);

    Assertions.assertTrue(icebergCatalog.asTableCatalog().tableExists(tableIdentifier));
    icebergCatalog.asTableCatalog().dropTable(tableIdentifier);
    Assertions.assertFalse(icebergCatalog.asTableCatalog().tableExists(tableIdentifier));
  }

  @Test
  public void testListTableException() {
    Namespace tableNs = Namespace.of("metalake", icebergCatalog.name(), "not_exist_db");
    Throwable exception =
        Assertions.assertThrows(
            NoSuchSchemaException.class, () -> icebergCatalog.asTableCatalog().listTables(tableNs));
    Assertions.assertTrue(exception.getMessage().contains("Schema (database) does not exist"));
  }

  @Test
  public void testAlterIcebergTable() {
    // create a table with random name
    NameIdentifier tableIdentifier =
        NameIdentifier.of(
            META_LAKE_NAME, icebergCatalog.name(), icebergSchema.name(), genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    IcebergColumn col1 =
        new IcebergColumn.Builder()
            .withName("col_1")
            .withType(TypeCreator.NULLABLE.I8)
            .withComment(ICEBERG_COMMENT)
            .build();
    IcebergColumn col2 =
        new IcebergColumn.Builder()
            .withName("col_2")
            .withType(TypeCreator.NULLABLE.DATE)
            .withComment(ICEBERG_COMMENT)
            .build();
    Column[] columns = new Column[] {col1, col2};

    Distribution distribution = Distribution.NONE;
    SortOrder[] sortOrders = createSortOrder();

    Table createdTable =
        icebergCatalog
            .asTableCatalog()
            .createTable(
                tableIdentifier,
                columns,
                ICEBERG_COMMENT,
                properties,
                new Transform[0],
                distribution,
                sortOrders);
    Assertions.assertTrue(icebergCatalog.asTableCatalog().tableExists(tableIdentifier));

    Throwable exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                icebergCatalog
                    .asTableCatalog()
                    .alterTable(
                        tableIdentifier,
                        TableChange.updateComment(ICEBERG_COMMENT + "_new"),
                        TableChange.rename("test_iceberg_table_new")));
    Assertions.assertTrue(
        exception.getMessage().contains("The operation to change the table name cannot"));

    // test alter
    icebergCatalog
        .asTableCatalog()
        .alterTable(
            tableIdentifier,
            TableChange.updateComment(ICEBERG_COMMENT + "_new"),
            TableChange.removeProperty("key1"),
            TableChange.setProperty("key2", "val2_new"),
            // columns current format: [col_1:I8:comment, col_2:DATE:comment]
            TableChange.addColumn(new String[] {"col_3"}, TypeCreator.NULLABLE.STRING),
            // columns current format: [col_1:I8:comment, col_2:DATE:comment, col_3:STRING:null]
            TableChange.renameColumn(new String[] {"col_2"}, "col_2_new"),
            // columns current format: [col_1:I8:comment, col_2_new:DATE:comment, col_3:STRING:null]
            TableChange.updateColumnComment(new String[] {"col_1"}, ICEBERG_COMMENT + "_new"),
            // columns current format: [col_1:I8:comment_new, col_2_new:DATE:comment,
            // col_3:STRING:null]
            TableChange.updateColumnType(new String[] {"col_1"}, TypeCreator.NULLABLE.I32),
            // columns current format: [col_1:I32:comment_new, col_2_new:DATE:comment,
            // col_3:STRING:null]
            TableChange.updateColumnPosition(
                new String[] {"col_2"}, TableChange.ColumnPosition.first())
            // columns current: [col_2_new:DATE:comment, col_1:I32:comment_new, col_3:STRING:null]
            );

    icebergCatalog
        .asTableCatalog()
        .alterTable(tableIdentifier, TableChange.rename("test_iceberg_table_new"));

    Table alteredTable =
        icebergCatalog
            .asTableCatalog()
            .loadTable(NameIdentifier.of(tableIdentifier.namespace(), "test_iceberg_table_new"));

    Assertions.assertEquals(ICEBERG_COMMENT + "_new", alteredTable.comment());
    Assertions.assertFalse(alteredTable.properties().containsKey("key1"));
    Assertions.assertEquals(alteredTable.properties().get("key2"), "val2_new");

    Assertions.assertEquals(sortOrders.length, alteredTable.sortOrder().length);

    Column[] expected =
        new Column[] {
          new IcebergColumn.Builder()
              .withName("col_2_new")
              .withType(TypeCreator.NULLABLE.DATE)
              .withComment(ICEBERG_COMMENT)
              .build(),
          new IcebergColumn.Builder()
              .withName("col_1")
              .withType(TypeCreator.NULLABLE.I32)
              .withComment(ICEBERG_COMMENT + "_new")
              .build(),
          new IcebergColumn.Builder()
              .withName("col_3")
              .withType(TypeCreator.NULLABLE.STRING)
              .withComment(null)
              .build()
        };
    Assertions.assertArrayEquals(expected, alteredTable.columns());

    // test delete column change
    icebergCatalog
        .asTableCatalog()
        .alterTable(
            NameIdentifier.of(tableIdentifier.namespace(), "test_iceberg_table_new"),
            TableChange.deleteColumn(new String[] {"col_1"}, false));
    Table alteredTable1 =
        icebergCatalog
            .asTableCatalog()
            .loadTable(NameIdentifier.of(tableIdentifier.namespace(), "test_iceberg_table_new"));
    expected =
        Arrays.stream(expected).filter(c -> !"col_1".equals(c.name())).toArray(Column[]::new);
    Assertions.assertArrayEquals(expected, alteredTable1.columns());

    Assertions.assertNotNull(alteredTable.partitioning());
    Assertions.assertArrayEquals(createdTable.partitioning(), alteredTable.partitioning());
  }

  @Test
  public void testTableProperty() {
    CatalogEntity entity = createDefaultCatalogEntity();
    try (IcebergCatalogOperations ops = new IcebergCatalogOperations(entity)) {
      ops.initialize(Maps.newHashMap());
      Map<String, String> map = Maps.newHashMap();
      map.put(IcebergTablePropertiesMetadata.COMMENT, "test");
      map.put(IcebergTablePropertiesMetadata.CREATOR, "test");
      map.put(IcebergTablePropertiesMetadata.LOCATION, "test");
      map.put(IcebergTablePropertiesMetadata.CURRENT_SNAPSHOT_ID, "test");
      map.put(IcebergTablePropertiesMetadata.CHERRY_PICK_SNAPSHOT_ID, "test");
      map.put(IcebergTablePropertiesMetadata.SORT_ORDER, "test");
      map.put(IcebergTablePropertiesMetadata.IDENTIFIER_FIELDS, "test");
      for (Map.Entry<String, String> entry : map.entrySet()) {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              ops.tablePropertiesMetadata()
                  .validatePropertyForCreate(
                      new HashMap<String, String>() {
                        {
                          put(entry.getKey(), entry.getValue());
                        }
                      });
            });
      }

      map = Maps.newHashMap();
      map.put("key1", "val1");
      map.put("key2", "val2");
      for (Map.Entry<String, String> entry : map.entrySet()) {
        Assertions.assertDoesNotThrow(
            () -> {
              ops.tablePropertiesMetadata()
                  .validatePropertyForCreate(
                      new HashMap<String, String>() {
                        {
                          put(entry.getKey(), entry.getValue());
                        }
                      });
            });
      }
    }
  }

  protected static String genRandomName() {
    return UUID.randomUUID().toString().replace("-", "");
  }
}
