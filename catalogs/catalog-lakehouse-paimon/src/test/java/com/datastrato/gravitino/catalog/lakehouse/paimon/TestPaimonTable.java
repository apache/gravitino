/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon;

import static com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonColumn.fromPaimonColumn;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.TestPaimonCatalog.PAIMON_PROPERTIES_METADATA;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.PropertiesMetadataHelpers;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestPaimonTable {

  private static final String META_LAKE_NAME = "metalake";

  private static final String PAIMON_CATALOG_NAME = "test_catalog";
  private static final String PAIMON_SCHEMA_NAME = "test_schema";
  private static final String PAIMON_COMMENT = "test_comment";
  private static PaimonCatalog paimonCatalog;
  private static PaimonCatalogOperations paimonCatalogOperations;
  private static PaimonSchema paimonSchema;
  private static final NameIdentifier schemaIdent =
      NameIdentifier.of(META_LAKE_NAME, PAIMON_CATALOG_NAME, PAIMON_SCHEMA_NAME);

  @BeforeAll
  static void setup() {
    initPaimonCatalog();
    initPaimonSchema();
  }

  @AfterEach
  void resetSchema() {
    NameIdentifier[] nameIdentifiers =
        paimonCatalogOperations.listTables(
            Namespace.of(ArrayUtils.add(schemaIdent.namespace().levels(), schemaIdent.name())));
    if (ArrayUtils.isNotEmpty(nameIdentifiers)) {
      Arrays.stream(nameIdentifiers)
          .map(
              nameIdentifier -> {
                String[] levels = nameIdentifier.namespace().levels();
                return NameIdentifier.of(
                    Namespace.of(levels[levels.length - 1]), nameIdentifier.name());
              })
          .forEach(nameIdentifier -> paimonCatalogOperations.dropTable(nameIdentifier));
    }
    paimonCatalogOperations.dropSchema(schemaIdent, false);
    initPaimonSchema();
  }

  @AfterAll
  static void cleanUp() {
    paimonCatalogOperations.dropSchema(schemaIdent, true);
  }

  private static CatalogEntity createDefaultCatalogEntity() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("testPaimonUser").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName(PAIMON_CATALOG_NAME)
            .withNamespace(Namespace.of(META_LAKE_NAME))
            .withType(PaimonCatalog.Type.RELATIONAL)
            .withProvider("lakehouse-paimon")
            .withAuditInfo(auditInfo)
            .build();
    return entity;
  }

  @Test
  void testCreatePaimonTable() {
    String paimonTableName = "test_paimon_table";
    NameIdentifier tableIdentifier = NameIdentifier.of(paimonSchema.name(), paimonTableName);
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    PaimonColumn col1 =
        fromPaimonColumn(new DataField(0, "col_1", DataTypes.INT().nullable(), PAIMON_COMMENT));
    PaimonColumn col2 =
        fromPaimonColumn(new DataField(1, "col_2", DataTypes.DATE().notNull(), PAIMON_COMMENT));
    RowType rowTypeInside =
        RowType.builder()
            .field("integer_field_inside", DataTypes.INT().notNull())
            .field("string_field_inside", DataTypes.STRING().notNull())
            .build();
    RowType rowType =
        RowType.builder()
            .field("integer_field", DataTypes.INT().notNull())
            .field("string_field", DataTypes.STRING().notNull(), "string field")
            .field("struct_field", rowTypeInside.nullable(), "struct field")
            .build();
    PaimonColumn col3 =
        fromPaimonColumn(new DataField(2, "col_3", rowType.notNull(), PAIMON_COMMENT));

    Column[] columns = new Column[] {col1, col2, col3};
    Table table =
        paimonCatalogOperations.createTable(
            tableIdentifier,
            columns,
            PAIMON_COMMENT,
            properties,
            new Transform[0],
            Distributions.NONE,
            new SortOrder[0]);

    Assertions.assertEquals(tableIdentifier.name(), table.name());
    Assertions.assertEquals(PAIMON_COMMENT, table.comment());
    Assertions.assertEquals("val1", table.properties().get("key1"));
    Assertions.assertEquals("val2", table.properties().get("key2"));

    Table loadedTable = paimonCatalogOperations.loadTable(tableIdentifier);

    Assertions.assertEquals("val1", loadedTable.properties().get("key1"));
    Assertions.assertEquals("val2", loadedTable.properties().get("key2"));
    Assertions.assertTrue(loadedTable.columns()[0].nullable());
    Assertions.assertFalse(loadedTable.columns()[1].nullable());
    Assertions.assertFalse(loadedTable.columns()[2].nullable());

    Assertions.assertTrue(paimonCatalogOperations.tableExists(tableIdentifier));
    NameIdentifier[] tableIdents = paimonCatalogOperations.listTables(tableIdentifier.namespace());
    Assertions.assertTrue(Arrays.asList(tableIdents).contains(tableIdentifier));

    // Test exception
    TableCatalog tableCatalog = paimonCatalogOperations;
    Throwable exception =
        Assertions.assertThrows(
            TableAlreadyExistsException.class,
            () ->
                tableCatalog.createTable(
                    tableIdentifier,
                    columns,
                    PAIMON_COMMENT,
                    properties,
                    new Transform[0],
                    Distributions.NONE,
                    new SortOrder[0]));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains(String.format("Paimon table %s already exists", tableIdentifier)));
  }

  @Test
  void testDropPaimonTable() {
    NameIdentifier tableIdentifier = NameIdentifier.of(paimonSchema.name(), genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    PaimonColumn col1 =
        fromPaimonColumn(new DataField(0, "col_1", DataTypes.INT().nullable(), PAIMON_COMMENT));
    PaimonColumn col2 =
        fromPaimonColumn(new DataField(1, "col_2", DataTypes.DATE().nullable(), PAIMON_COMMENT));
    Column[] columns = new Column[] {col1, col2};

    paimonCatalogOperations.createTable(
        tableIdentifier,
        columns,
        PAIMON_COMMENT,
        properties,
        new Transform[0],
        Distributions.NONE,
        new SortOrder[0]);

    Assertions.assertTrue(paimonCatalogOperations.tableExists(tableIdentifier));
    paimonCatalogOperations.dropTable(tableIdentifier);
    Assertions.assertFalse(paimonCatalogOperations.tableExists(tableIdentifier));
  }

  @Test
  void testListTableException() {
    Namespace tableNs = Namespace.of("metalake", paimonCatalog.name(), "not_exist_db");
    TableCatalog tableCatalog = paimonCatalogOperations;
    Throwable exception =
        Assertions.assertThrows(
            NoSuchSchemaException.class, () -> tableCatalog.listTables(tableNs));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains(
                String.format("Paimon schema (database) %s does not exist", tableNs.toString())));
  }

  @Test
  void testTableProperty() throws IOException {
    CatalogEntity entity = createDefaultCatalogEntity();
    try (PaimonCatalogOperations ops = new PaimonCatalogOperations()) {
      ops.initialize(
          initBackendCatalogProperties(), entity.toCatalogInfo(), PAIMON_PROPERTIES_METADATA);
      Map<String, String> map = Maps.newHashMap();
      map.put(PaimonTablePropertiesMetadata.COMMENT, "test");
      map.put(PaimonTablePropertiesMetadata.CREATOR, "test");
      for (Map.Entry<String, String> entry : map.entrySet()) {
        HashMap<String, String> properties =
            new HashMap<String, String>() {
              {
                put(entry.getKey(), entry.getValue());
              }
            };
        PropertiesMetadata metadata = paimonCatalog.tablePropertiesMetadata();
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> PropertiesMetadataHelpers.validatePropertyForCreate(metadata, properties));
      }

      map = Maps.newHashMap();
      map.put("key1", "val1");
      map.put("key2", "val2");
      for (Map.Entry<String, String> entry : map.entrySet()) {
        HashMap<String, String> properties =
            new HashMap<String, String>() {
              {
                put(entry.getKey(), entry.getValue());
              }
            };
        PropertiesMetadata metadata = paimonCatalog.tablePropertiesMetadata();
        Assertions.assertDoesNotThrow(
            () -> {
              PropertiesMetadataHelpers.validatePropertyForCreate(metadata, properties);
            });
      }
    }
  }

  private static String genRandomName() {
    return UUID.randomUUID().toString().replace("-", "");
  }

  private static Map<String, String> initBackendCatalogProperties() {
    Map<String, String> conf = Maps.newHashMap();
    conf.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, "filesystem");
    conf.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, "/tmp/paimon_catalog_warehouse");
    return conf;
  }

  private static void initPaimonCatalog() {
    CatalogEntity entity = createDefaultCatalogEntity();

    Map<String, String> conf = initBackendCatalogProperties();
    paimonCatalog = new PaimonCatalog().withCatalogConf(conf).withCatalogEntity(entity);
    paimonCatalogOperations = (PaimonCatalogOperations) paimonCatalog.ops();
  }

  private static void initPaimonSchema() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    if (paimonCatalogOperations.schemaExists(schemaIdent)) {
      paimonCatalogOperations.dropSchema(schemaIdent, true);
    }
    paimonSchema = paimonCatalogOperations.createSchema(schemaIdent, PAIMON_COMMENT, properties);
  }
}
