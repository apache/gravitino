/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.catalog.hive.miniHMS.MiniHiveMetastoreService;
import com.datastrato.graviton.exceptions.TableAlreadyExistsException;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.CatalogEntity;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.TableChange;
import com.google.common.collect.Maps;
import io.substrait.type.TypeCreator;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class HiveTableTest extends MiniHiveMetastoreService {

  private static final String META_LAKE_NAME = "metalake";

  private static final String HIVE_CATALOG_NAME = "test_catalog";
  private static final String HIVE_SCHEMA_NAME = "test_schema";
  private static final String HIVE_COMMENT = "test_comment";
  private static HiveCatalog hiveCatalog;
  private static HiveSchema hiveSchema;

  @BeforeAll
  private static void setup() {
    initHiveCatalog();
    initHiveSchema();
  }

  @AfterEach
  private void resetSchema() {
    hiveCatalog.asSchemas().dropSchema(hiveSchema.nameIdentifier(), true);
    initHiveSchema();
  }

  private static void initHiveSchema() {
    NameIdentifier schemaIdent =
        NameIdentifier.of(META_LAKE_NAME, hiveCatalog.name(), HIVE_SCHEMA_NAME);
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    hiveSchema =
        (HiveSchema) hiveCatalog.asSchemas().createSchema(schemaIdent, HIVE_COMMENT, properties);
  }

  private static void initHiveCatalog() {
    AuditInfo auditInfo =
        new AuditInfo.Builder().withCreator("testHiveUser").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        new CatalogEntity.Builder()
            .withId(1L)
            .withName(HIVE_CATALOG_NAME)
            .withNamespace(Namespace.of(META_LAKE_NAME))
            .withType(HiveCatalog.Type.RELATIONAL)
            .withMetalakeId(1L)
            .withAuditInfo(auditInfo)
            .build();

    Map<String, String> conf = Maps.newHashMap();
    metastore.hiveConf().forEach(e -> conf.put(e.getKey(), e.getValue()));
    hiveCatalog = new HiveCatalog().withCatalogConf(conf).withCatalogEntity(entity);
  }

  @Test
  public void testCreateHiveTable() {
    Long tableId = 1L;
    String hiveTableName = "test_hive_table";
    NameIdentifier tableIdentifier =
        NameIdentifier.of(hiveSchema.nameIdentifier().toString(), hiveTableName);
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    HiveColumn col1 =
        new HiveColumn.Builder()
            .withName("col_1")
            .withType(TypeCreator.NULLABLE.I8)
            .withComment(HIVE_COMMENT)
            .build();
    HiveColumn col2 =
        new HiveColumn.Builder()
            .withName("col_2")
            .withType(TypeCreator.NULLABLE.DATE)
            .withComment(HIVE_COMMENT)
            .build();
    Column[] columns = new Column[] {col1, col2};

    Table table =
        hiveCatalog
            .asTableCatalog()
            .createTable(tableIdentifier, columns, HIVE_COMMENT, properties);
    Assertions.assertEquals(tableIdentifier.name(), table.name());
    Assertions.assertEquals(HIVE_COMMENT, table.comment());
    Assertions.assertArrayEquals(columns, table.columns());

    Assertions.assertTrue(hiveCatalog.asTableCatalog().tableExists(tableIdentifier));
    NameIdentifier[] tableIdents =
        hiveCatalog.asTableCatalog().listTables(tableIdentifier.namespace());
    Assertions.assertTrue(Arrays.asList(tableIdents).contains(tableIdentifier));

    // Test exception
    Throwable exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                hiveCatalog
                    .asTableCatalog()
                    .createTable(hiveSchema.nameIdentifier(), columns, HIVE_COMMENT, properties));
    Assertions.assertTrue(
        exception.getMessage().contains("Cannot support invalid namespace in Hive Metastore"));

    exception =
        Assertions.assertThrows(
            TableAlreadyExistsException.class,
            () ->
                hiveCatalog
                    .asTableCatalog()
                    .createTable(tableIdentifier, columns, HIVE_COMMENT, properties));
    Assertions.assertTrue(exception.getMessage().contains("Table already exists"));
  }

  @Test
  public void testDropHiveTable() {
    Long tableId = 1L;
    NameIdentifier tableIdentifier =
        NameIdentifier.of(hiveSchema.nameIdentifier().toString(), genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    HiveColumn col1 =
        new HiveColumn.Builder()
            .withName("col_1")
            .withType(TypeCreator.NULLABLE.I8)
            .withComment(HIVE_COMMENT)
            .build();
    HiveColumn col2 =
        new HiveColumn.Builder()
            .withName("col_2")
            .withType(TypeCreator.NULLABLE.DATE)
            .withComment(HIVE_COMMENT)
            .build();
    Column[] columns = new Column[] {col1, col2};

    hiveCatalog.asTableCatalog().createTable(tableIdentifier, columns, HIVE_COMMENT, properties);

    Assertions.assertTrue(hiveCatalog.asTableCatalog().tableExists(tableIdentifier));
    hiveCatalog.asTableCatalog().dropTable(tableIdentifier);
    Assertions.assertFalse(hiveCatalog.asTableCatalog().tableExists(tableIdentifier));
  }

  @Test
  public void testListTableException() {
    Throwable exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> hiveCatalog.asTableCatalog().listTables(hiveSchema.nameIdentifier().namespace()));
    Assertions.assertTrue(
        exception.getMessage().contains("Cannot support invalid namespace in Hive Metastore"));

    NameIdentifier ident = NameIdentifier.of("metalake", hiveCatalog.name(), "not_exist_db");
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> hiveCatalog.asTableCatalog().listTables(ident.namespace()));
    Assertions.assertTrue(
        exception.getMessage().contains("Cannot support invalid namespace in Hive Metastore"));
  }

  @Test
  public void testAlterHiveTable() {
    Long tableId = 1L;
    NameIdentifier tableIdentifier =
        NameIdentifier.of(hiveSchema.nameIdentifier().toString(), genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    HiveColumn col1 =
        new HiveColumn.Builder()
            .withName("col_1")
            .withType(TypeCreator.NULLABLE.I8)
            .withComment(HIVE_COMMENT)
            .build();
    HiveColumn col2 =
        new HiveColumn.Builder()
            .withName("col_2")
            .withType(TypeCreator.NULLABLE.DATE)
            .withComment(HIVE_COMMENT)
            .build();
    Column[] columns = new Column[] {col1, col2};

    hiveCatalog.asTableCatalog().createTable(tableIdentifier, columns, HIVE_COMMENT, properties);
    Assertions.assertTrue(hiveCatalog.asTableCatalog().tableExists(tableIdentifier));

    hiveCatalog
        .asTableCatalog()
        .alterTable(
            tableIdentifier,
            TableChange.rename("test_hive_table_new"),
            TableChange.updateComment(HIVE_COMMENT + "_new"),
            TableChange.removeProperty("key1"),
            TableChange.setProperty("key2", "val2_new"),
            // columns current: [col_1:I8:comment, col_2:DATE:comment]
            TableChange.addColumn(new String[] {"col_3"}, TypeCreator.NULLABLE.STRING),
            // columns current: [col_1:I8:comment, col_2:DATE:comment, col_3:STRING:null]
            TableChange.renameColumn(new String[] {"col_2"}, "col_2_new"),
            // columns current: [col_1:I8:comment, col_2_new:DATE:comment, col_3:STRING:null]
            TableChange.updateColumnComment(new String[] {"col_1"}, HIVE_COMMENT + "_new"),
            // columns current: [col_1:I8:comment_new, col_2_new:DATE:comment, col_3:STRING:null]
            TableChange.updateColumnType(new String[] {"col_1"}, TypeCreator.NULLABLE.I32),
            // columns current: [col_1:I32:comment_new, col_2_new:DATE:comment, col_3:STRING:null]
            TableChange.updateColumnPosition(
                new String[] {"col_2_new"}, TableChange.ColumnPosition.first())
            // columns current: [col_2_new:DATE:comment, col_1:I32:comment_new, col_3:STRING:null]
            );
    Table alteredTable =
        hiveCatalog
            .asTableCatalog()
            .loadTable(NameIdentifier.of(tableIdentifier.namespace(), "test_hive_table_new"));

    Assertions.assertEquals(HIVE_COMMENT + "_new", alteredTable.comment());
    Assertions.assertFalse(alteredTable.properties().containsKey("key1"));
    Assertions.assertEquals(alteredTable.properties().get("key2"), "val2_new");

    Column[] expected =
        new Column[] {
          new HiveColumn.Builder()
              .withName("col_2_new")
              .withType(TypeCreator.NULLABLE.DATE)
              .withComment(HIVE_COMMENT)
              .build(),
          new HiveColumn.Builder()
              .withName("col_1")
              .withType(TypeCreator.NULLABLE.I32)
              .withComment(HIVE_COMMENT + "_new")
              .build(),
          new HiveColumn.Builder()
              .withName("col_3")
              .withType(TypeCreator.NULLABLE.STRING)
              .withComment(null)
              .build()
        };
    Assertions.assertArrayEquals(expected, alteredTable.columns());

    hiveCatalog
        .asTableCatalog()
        .alterTable(
            NameIdentifier.of(tableIdentifier.namespace(), "test_hive_table_new"),
            TableChange.deleteColumn(new String[] {"col_1"}, false));
    alteredTable =
        hiveCatalog
            .asTableCatalog()
            .loadTable(NameIdentifier.of(tableIdentifier.namespace(), "test_hive_table_new"));
    expected =
        Arrays.stream(expected).filter(c -> !"col_1".equals(c.name())).toArray(Column[]::new);
    Assertions.assertArrayEquals(expected, alteredTable.columns());
  }
}
