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
package org.apache.gravitino.catalog.lakehouse.paimon;

import static org.apache.gravitino.catalog.lakehouse.paimon.GravitinoPaimonColumn.fromPaimonColumn;
import static org.apache.gravitino.catalog.lakehouse.paimon.GravitinoPaimonTable.PAIMON_PRIMARY_KEY_INDEX_NAME;
import static org.apache.gravitino.catalog.lakehouse.paimon.TestPaimonCatalog.PAIMON_PROPERTIES_METADATA;
import static org.apache.gravitino.catalog.lakehouse.paimon.utils.TableOpsUtils.checkColumnCapability;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.identity;
import static org.apache.gravitino.rel.indexes.Indexes.primary;

import com.google.common.collect.Maps;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.PropertiesMetadataHelpers;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.types.Types;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestGravitinoPaimonTable {

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
          .forEach(nameIdentifier -> paimonCatalogOperations.purgeTable(nameIdentifier));
    }
    paimonCatalogOperations.dropSchema(schemaIdent, false);
    initPaimonSchema();
  }

  @AfterAll
  static void cleanUp() {
    paimonCatalogOperations.dropSchema(schemaIdent, true);
    String warehousePath = "/tmp/paimon_catalog_warehouse";
    try {
      FileUtils.deleteDirectory(new File(warehousePath));
      Files.delete(Paths.get(warehousePath));
    } catch (Exception e) {
      // Ignore
    }
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

    GravitinoPaimonColumn col1 =
        fromPaimonColumn(new DataField(0, "col_1", DataTypes.INT().nullable(), PAIMON_COMMENT));
    GravitinoPaimonColumn col2 =
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
    GravitinoPaimonColumn col3 =
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
  void testCreatePaimonPartitionedTable() {
    String paimonTableName = "test_paimon_partitioned_table";
    NameIdentifier tableIdentifier = NameIdentifier.of(paimonSchema.name(), paimonTableName);
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    GravitinoPaimonColumn col1 =
        fromPaimonColumn(new DataField(0, "col_1", DataTypes.INT().nullable(), PAIMON_COMMENT));
    GravitinoPaimonColumn col2 =
        fromPaimonColumn(new DataField(1, "col_2", DataTypes.DATE().notNull(), PAIMON_COMMENT));
    GravitinoPaimonColumn col3 =
        fromPaimonColumn(new DataField(2, "col_3", DataTypes.STRING().notNull(), PAIMON_COMMENT));
    Column[] columns = new Column[] {col1, col2, col3};

    Transform[] transforms = new Transform[] {identity("col_1"), identity("col_2")};
    String[] partitionKeys = new String[] {"col_1", "col_2"};

    Table table =
        paimonCatalogOperations.createTable(
            tableIdentifier,
            columns,
            PAIMON_COMMENT,
            properties,
            transforms,
            Distributions.NONE,
            new SortOrder[0]);

    Assertions.assertEquals(tableIdentifier.name(), table.name());
    Assertions.assertEquals(PAIMON_COMMENT, table.comment());
    Assertions.assertEquals("val1", table.properties().get("key1"));
    Assertions.assertEquals("val2", table.properties().get("key2"));
    Assertions.assertArrayEquals(transforms, table.partitioning());

    Table loadedTable = paimonCatalogOperations.loadTable(tableIdentifier);

    Assertions.assertEquals("val1", loadedTable.properties().get("key1"));
    Assertions.assertEquals("val2", loadedTable.properties().get("key2"));
    Assertions.assertTrue(loadedTable.columns()[0].nullable());
    Assertions.assertFalse(loadedTable.columns()[1].nullable());
    Assertions.assertFalse(loadedTable.columns()[2].nullable());
    Assertions.assertArrayEquals(transforms, loadedTable.partitioning());
    String[] loadedPartitionKeys =
        Arrays.stream(loadedTable.partitioning())
            .map(
                transform -> {
                  NamedReference[] references = transform.references();
                  Assertions.assertTrue(
                      references.length == 1
                          && references[0] instanceof NamedReference.FieldReference);
                  NamedReference.FieldReference fieldReference =
                      (NamedReference.FieldReference) references[0];
                  return fieldReference.fieldName()[0];
                })
            .toArray(String[]::new);
    Assertions.assertArrayEquals(partitionKeys, loadedPartitionKeys);
  }

  @Test
  void testCreatePaimonPrimaryKeyTable() {
    String paimonTableName = "test_paimon_primary_key_table";
    NameIdentifier tableIdentifier = NameIdentifier.of(paimonSchema.name(), paimonTableName);
    Map<String, String> properties = Maps.newHashMap();

    GravitinoPaimonColumn col1 =
        fromPaimonColumn(new DataField(0, "col_1", DataTypes.INT().notNull(), PAIMON_COMMENT));
    GravitinoPaimonColumn col2 =
        fromPaimonColumn(new DataField(1, "col_2", DataTypes.STRING().notNull(), PAIMON_COMMENT));
    GravitinoPaimonColumn col3 =
        fromPaimonColumn(new DataField(2, "col_3", DataTypes.STRING().nullable(), PAIMON_COMMENT));
    Column[] columns = new Column[] {col1, col2, col3};

    Transform[] transforms = new Transform[] {identity("col_1")};
    String[] partitionKeys = new String[] {"col_1"};
    Index[] indexes =
        Collections.singletonList(
                primary(PAIMON_PRIMARY_KEY_INDEX_NAME, new String[][] {new String[] {"col_2"}}))
            .toArray(new Index[0]);

    Table table =
        paimonCatalogOperations.createTable(
            tableIdentifier,
            columns,
            PAIMON_COMMENT,
            properties,
            transforms,
            Distributions.NONE,
            new SortOrder[0],
            indexes);

    Assertions.assertEquals(tableIdentifier.name(), table.name());
    Assertions.assertEquals(PAIMON_COMMENT, table.comment());
    Assertions.assertEquals(properties, table.properties());
    Assertions.assertArrayEquals(transforms, table.partitioning());
    Assertions.assertEquals(indexes.length, table.index().length);
    for (int i = 0; i < indexes.length; i++) {
      Assertions.assertEquals(indexes[i].name(), table.index()[i].name());
      Assertions.assertEquals(indexes[i].type(), table.index()[i].type());
      Assertions.assertEquals(indexes[i].fieldNames(), table.index()[i].fieldNames());
    }

    Table loadedTable = paimonCatalogOperations.loadTable(tableIdentifier);
    Assertions.assertArrayEquals(transforms, loadedTable.partitioning());
    String[] loadedPartitionKeys =
        Arrays.stream(loadedTable.partitioning())
            .map(
                transform -> {
                  NamedReference[] references = transform.references();
                  Assertions.assertTrue(
                      references.length == 1
                          && references[0] instanceof NamedReference.FieldReference);
                  NamedReference.FieldReference fieldReference =
                      (NamedReference.FieldReference) references[0];
                  return fieldReference.fieldName()[0];
                })
            .toArray(String[]::new);
    Assertions.assertArrayEquals(partitionKeys, loadedPartitionKeys);
    Assertions.assertEquals(indexes.length, loadedTable.index().length);
    for (int i = 0; i < indexes.length; i++) {
      Assertions.assertEquals(indexes[i].name(), loadedTable.index()[i].name());
      Assertions.assertEquals(indexes[i].type(), loadedTable.index()[i].type());
      Assertions.assertArrayEquals(indexes[i].fieldNames(), loadedTable.index()[i].fieldNames());
    }
  }

  @Test
  void testDropPaimonTable() {
    NameIdentifier tableIdentifier = NameIdentifier.of(paimonSchema.name(), genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    GravitinoPaimonColumn col1 =
        fromPaimonColumn(new DataField(0, "col_1", DataTypes.INT().nullable(), PAIMON_COMMENT));
    GravitinoPaimonColumn col2 =
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
    paimonCatalogOperations.purgeTable(tableIdentifier);
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
  void testTableProperty() {
    CatalogEntity entity = createDefaultCatalogEntity();
    try (PaimonCatalogOperations ops = new PaimonCatalogOperations()) {
      ops.initialize(
          initBackendCatalogProperties(), entity.toCatalogInfo(), PAIMON_PROPERTIES_METADATA);
      HashMap<String, String> reservedProps =
          new HashMap<String, String>() {
            {
              put(PaimonTablePropertiesMetadata.COMMENT, "test");
              put(PaimonTablePropertiesMetadata.OWNER, "test");
              put(PaimonTablePropertiesMetadata.BUCKET_KEY, "test");
              put(PaimonTablePropertiesMetadata.PRIMARY_KEY, "test");
              put(PaimonTablePropertiesMetadata.PARTITION, "test");
            }
          };
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              PropertiesMetadataHelpers.validatePropertyForCreate(
                  paimonCatalog.tablePropertiesMetadata(), reservedProps));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              PropertiesMetadataHelpers.validatePropertyForAlter(
                  paimonCatalog.tablePropertiesMetadata(), reservedProps, Collections.emptyMap()));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              PropertiesMetadataHelpers.validatePropertyForAlter(
                  paimonCatalog.tablePropertiesMetadata(), Collections.emptyMap(), reservedProps));

      Map<String, String> immutableProps =
          new HashMap<String, String>() {
            {
              put(PaimonTablePropertiesMetadata.MERGE_ENGINE, "test");
              put(PaimonTablePropertiesMetadata.SEQUENCE_FIELD, "test");
              put(PaimonTablePropertiesMetadata.ROWKIND_FIELD, "test");
            }
          };
      for (Map.Entry<String, String> entry : immutableProps.entrySet()) {
        HashMap<String, String> properties =
            new HashMap<String, String>() {
              {
                put(entry.getKey(), entry.getValue());
              }
            };
        PropertiesMetadata metadata = paimonCatalog.tablePropertiesMetadata();
        Assertions.assertDoesNotThrow(
            () -> PropertiesMetadataHelpers.validatePropertyForCreate(metadata, properties));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                PropertiesMetadataHelpers.validatePropertyForAlter(
                    metadata, properties, Collections.emptyMap()));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                PropertiesMetadataHelpers.validatePropertyForAlter(
                    metadata, Collections.emptyMap(), properties));
      }
    }
  }

  @Test
  void testGravitinoToPaimonTable() {
    Column[] columns = createColumns();
    NameIdentifier identifier = NameIdentifier.of("test_schema", "test_table");
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");

    GravitinoPaimonTable gravitinoPaimonTable =
        GravitinoPaimonTable.builder()
            .withName(identifier.name())
            .withColumns(
                Arrays.stream(columns)
                    .map(
                        column -> {
                          checkColumnCapability(
                              column.name(), column.defaultValue(), column.autoIncrement());
                          return GravitinoPaimonColumn.builder()
                              .withName(column.name())
                              .withType(column.dataType())
                              .withComment(column.comment())
                              .withNullable(column.nullable())
                              .withAutoIncrement(column.autoIncrement())
                              .withDefaultValue(column.defaultValue())
                              .build();
                        })
                    .toArray(GravitinoPaimonColumn[]::new))
            .withComment("test_table_comment")
            .withProperties(properties)
            .build();
    Schema paimonTableSchema = gravitinoPaimonTable.toPaimonTableSchema();
    Assertions.assertEquals(gravitinoPaimonTable.comment(), gravitinoPaimonTable.comment());
    Assertions.assertEquals(gravitinoPaimonTable.properties(), paimonTableSchema.options());
    Assertions.assertEquals(
        gravitinoPaimonTable.columns().length, paimonTableSchema.fields().size());
    Assertions.assertEquals(3, paimonTableSchema.fields().size());
    for (int i = 0; i < gravitinoPaimonTable.columns().length; i++) {
      Column column = gravitinoPaimonTable.columns()[i];
      DataField dataField = paimonTableSchema.fields().get(i);
      Assertions.assertEquals(column.name(), dataField.name());
      Assertions.assertEquals(column.comment(), dataField.description());
    }
    Assertions.assertEquals(new IntType().nullable(), paimonTableSchema.fields().get(0).type());
    Assertions.assertEquals(new DateType().nullable(), paimonTableSchema.fields().get(1).type());
    Assertions.assertEquals(
        new VarCharType(Integer.MAX_VALUE).notNull(), paimonTableSchema.fields().get(2).type());
  }

  @Test
  public void testGravitinoToPaimonTableWithPartitions() {
    Column[] columns = createColumns();
    NameIdentifier identifier = NameIdentifier.of("test_schema", "test_partitioned_table");
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");

    Transform[] partitions =
        new Transform[] {identity(columns[0].name()), identity(columns[1].name())};
    List<String> partitionKeys = Arrays.asList(columns[0].name(), columns[1].name());

    GravitinoPaimonTable gravitinoPaimonTable =
        GravitinoPaimonTable.builder()
            .withName(identifier.name())
            .withColumns(
                Arrays.stream(columns)
                    .map(
                        column -> {
                          checkColumnCapability(
                              column.name(), column.defaultValue(), column.autoIncrement());
                          return GravitinoPaimonColumn.builder()
                              .withName(column.name())
                              .withType(column.dataType())
                              .withComment(column.comment())
                              .withNullable(column.nullable())
                              .withAutoIncrement(column.autoIncrement())
                              .withDefaultValue(column.defaultValue())
                              .build();
                        })
                    .toArray(GravitinoPaimonColumn[]::new))
            .withPartitioning(partitions)
            .withComment("test_table_comment")
            .withProperties(properties)
            .build();
    Schema paimonTableSchema = gravitinoPaimonTable.toPaimonTableSchema();
    Assertions.assertArrayEquals(
        partitionKeys.toArray(new String[0]),
        paimonTableSchema.partitionKeys().toArray(new String[0]));
    Assertions.assertEquals(gravitinoPaimonTable.comment(), gravitinoPaimonTable.comment());
    Assertions.assertEquals(gravitinoPaimonTable.properties(), paimonTableSchema.options());
    Assertions.assertEquals(
        gravitinoPaimonTable.columns().length, paimonTableSchema.fields().size());
    Assertions.assertEquals(3, paimonTableSchema.fields().size());
    for (int i = 0; i < gravitinoPaimonTable.columns().length; i++) {
      Column column = gravitinoPaimonTable.columns()[i];
      DataField dataField = paimonTableSchema.fields().get(i);
      Assertions.assertEquals(column.name(), dataField.name());
      Assertions.assertEquals(column.comment(), dataField.description());
    }
    Assertions.assertEquals(new IntType().nullable(), paimonTableSchema.fields().get(0).type());
    Assertions.assertEquals(new DateType().nullable(), paimonTableSchema.fields().get(1).type());
    Assertions.assertEquals(
        new VarCharType(Integer.MAX_VALUE).notNull(), paimonTableSchema.fields().get(2).type());
  }

  @Test
  public void testGravitinoToPaimonTableWithPrimaryKey() {
    Column[] columns = createColumns();
    NameIdentifier identifier = NameIdentifier.of("test_schema", "test_primary_key_table");
    Map<String, String> properties = Maps.newHashMap();

    Transform[] partitions = new Transform[] {identity(columns[0].name())};
    List<String> partitionKeys = Collections.singletonList(columns[0].name());
    List<String> primaryKeys = Collections.singletonList(columns[2].name());
    Index[] indexes =
        Collections.singletonList(
                primary(
                    PAIMON_PRIMARY_KEY_INDEX_NAME,
                    new String[][] {new String[] {columns[2].name()}}))
            .toArray(new Index[0]);

    GravitinoPaimonTable gravitinoPaimonTable =
        GravitinoPaimonTable.builder()
            .withName(identifier.name())
            .withColumns(
                Arrays.stream(columns)
                    .map(
                        column -> {
                          checkColumnCapability(
                              column.name(), column.defaultValue(), column.autoIncrement());
                          return GravitinoPaimonColumn.builder()
                              .withName(column.name())
                              .withType(column.dataType())
                              .withComment(column.comment())
                              .withNullable(column.nullable())
                              .withAutoIncrement(column.autoIncrement())
                              .withDefaultValue(column.defaultValue())
                              .build();
                        })
                    .toArray(GravitinoPaimonColumn[]::new))
            .withPartitioning(partitions)
            .withComment("test_table_comment")
            .withProperties(properties)
            .withIndexes(indexes)
            .build();
    Schema paimonTableSchema = gravitinoPaimonTable.toPaimonTableSchema();
    Assertions.assertArrayEquals(
        partitionKeys.toArray(new String[0]),
        paimonTableSchema.partitionKeys().toArray(new String[0]));
    Assertions.assertEquals(
        gravitinoPaimonTable.columns().length, paimonTableSchema.fields().size());
    Assertions.assertEquals(3, paimonTableSchema.fields().size());
    for (int i = 0; i < gravitinoPaimonTable.columns().length; i++) {
      Column column = gravitinoPaimonTable.columns()[i];
      DataField dataField = paimonTableSchema.fields().get(i);
      Assertions.assertEquals(column.name(), dataField.name());
      Assertions.assertEquals(column.comment(), dataField.description());
    }
    Assertions.assertEquals(new IntType().nullable(), paimonTableSchema.fields().get(0).type());
    Assertions.assertEquals(new DateType().nullable(), paimonTableSchema.fields().get(1).type());
    Assertions.assertEquals(
        new VarCharType(Integer.MAX_VALUE).notNull(), paimonTableSchema.fields().get(2).type());
    Assertions.assertArrayEquals(
        primaryKeys.toArray(new String[0]), paimonTableSchema.primaryKeys().toArray(new String[0]));
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

  private static Column[] createColumns() {
    Column col1 = Column.of("col1", Types.IntegerType.get(), "col_1_comment");
    Column col2 = Column.of("col2", Types.DateType.get(), "col_2_comment");
    Column col3 =
        Column.of(
            "col3", Types.StringType.get(), "col_3_comment", false, false, DEFAULT_VALUE_NOT_SET);
    return new Column[] {col1, col2, col3};
  }
}
