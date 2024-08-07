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

import static org.apache.gravitino.rel.expressions.transforms.Transforms.bucket;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.day;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.identity;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.truncate;

import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.types.Types;
import org.apache.iceberg.DistributionMode;
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
  private static IcebergCatalogOperations icebergCatalogOperations;
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
    NameIdentifier[] nameIdentifiers =
        icebergCatalogOperations.listTables(
            Namespace.of(ArrayUtils.add(schemaIdent.namespace().levels(), schemaIdent.name())));
    if (ArrayUtils.isNotEmpty(nameIdentifiers)) {
      Arrays.stream(nameIdentifiers).forEach(icebergCatalogOperations::dropTable);
    }
    icebergCatalogOperations.dropSchema(schemaIdent, false);
    initIcebergSchema();
  }

  private static void initIcebergSchema() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    icebergSchema = icebergCatalogOperations.createSchema(schemaIdent, ICEBERG_COMMENT, properties);
  }

  private static void initIcebergCatalog() {
    CatalogEntity entity = createDefaultCatalogEntity();

    Map<String, String> conf = Maps.newHashMap();
    icebergCatalog = new IcebergCatalog().withCatalogConf(conf).withCatalogEntity(entity);
    icebergCatalogOperations = (IcebergCatalogOperations) icebergCatalog.ops();
  }

  private static CatalogEntity createDefaultCatalogEntity() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("testIcebergUser").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        CatalogEntity.builder()
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
      SortOrders.of(
          NamedReference.field("col_2"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST),
      SortOrders.of(
          FunctionExpression.of(
              "bucket", Literals.integerLiteral(10), NamedReference.field("col_1")),
          SortDirection.DESCENDING,
          NullOrdering.NULLS_FIRST),
      SortOrders.of(
          FunctionExpression.of(
              "truncate", Literals.integerLiteral(1), NamedReference.field("col_1")),
          SortDirection.DESCENDING,
          NullOrdering.NULLS_FIRST)
    };
  }

  @Test
  public void testCreateIcebergTable() {
    String icebergTableName = "test_iceberg_table";
    NameIdentifier tableIdentifier =
        NameIdentifier.of(
            META_LAKE_NAME, icebergCatalog.name(), icebergSchema.name(), icebergTableName);
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    IcebergColumn col1 =
        IcebergColumn.builder()
            .withName("col_1")
            .withType(Types.IntegerType.get())
            .withComment(ICEBERG_COMMENT)
            .withNullable(true)
            .build();
    IcebergColumn col2 =
        IcebergColumn.builder()
            .withName("col_2")
            .withType(Types.DateType.get())
            .withComment(ICEBERG_COMMENT)
            .withNullable(false)
            .build();
    Types.StructType structTypeInside =
        Types.StructType.of(
            Types.StructType.Field.notNullField("integer_field_inside", Types.IntegerType.get()),
            Types.StructType.Field.notNullField(
                "string_field_inside", Types.StringType.get(), "string field inside"));
    Types.StructType structType =
        Types.StructType.of(
            Types.StructType.Field.notNullField("integer_field", Types.IntegerType.get()),
            Types.StructType.Field.notNullField(
                "string_field", Types.StringType.get(), "string field"),
            Types.StructType.Field.nullableField("struct_field", structTypeInside, "struct field"));
    IcebergColumn col3 =
        IcebergColumn.builder()
            .withName("col_3")
            .withType(structType)
            .withComment(ICEBERG_COMMENT)
            .withNullable(false)
            .build();
    Column[] columns = new Column[] {col1, col2, col3};

    SortOrder[] sortOrders = createSortOrder();
    Table table =
        icebergCatalogOperations.createTable(
            tableIdentifier,
            columns,
            ICEBERG_COMMENT,
            properties,
            new Transform[0],
            Distributions.NONE,
            sortOrders);
    Assertions.assertEquals(tableIdentifier.name(), table.name());
    Assertions.assertEquals(ICEBERG_COMMENT, table.comment());
    Assertions.assertEquals("val1", table.properties().get("key1"));
    Assertions.assertEquals("val2", table.properties().get("key2"));

    Table loadedTable = icebergCatalogOperations.loadTable(tableIdentifier);

    Assertions.assertEquals("val1", loadedTable.properties().get("key1"));
    Assertions.assertEquals("val2", loadedTable.properties().get("key2"));
    Assertions.assertTrue(loadedTable.columns()[0].nullable());
    Assertions.assertFalse(loadedTable.columns()[1].nullable());
    Assertions.assertFalse(loadedTable.columns()[2].nullable());

    Assertions.assertTrue(icebergCatalogOperations.tableExists(tableIdentifier));
    NameIdentifier[] tableIdents = icebergCatalogOperations.listTables(tableIdentifier.namespace());
    Assertions.assertTrue(Arrays.asList(tableIdents).contains(tableIdentifier));

    Assertions.assertEquals(sortOrders.length, loadedTable.sortOrder().length);
    for (int i = 0; i < loadedTable.sortOrder().length; i++) {
      Assertions.assertEquals(sortOrders[i].direction(), loadedTable.sortOrder()[i].direction());
      Assertions.assertEquals(
          (sortOrders[i]).nullOrdering(), loadedTable.sortOrder()[i].nullOrdering());
      Assertions.assertEquals(
          (sortOrders[i]).expression(), loadedTable.sortOrder()[i].expression());
    }
    // Compare sort and order

    // Test exception
    TableCatalog tableCatalog = icebergCatalogOperations;
    Throwable exception =
        Assertions.assertThrows(
            TableAlreadyExistsException.class,
            () ->
                tableCatalog.createTable(
                    tableIdentifier,
                    columns,
                    ICEBERG_COMMENT,
                    properties,
                    new Transform[0],
                    Distributions.NONE,
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
        IcebergColumn.builder()
            .withName("city")
            .withType(Types.IntegerType.get())
            .withComment(ICEBERG_COMMENT)
            .build();
    IcebergColumn col2 =
        IcebergColumn.builder()
            .withName("date")
            .withType(Types.DateType.get())
            .withComment(ICEBERG_COMMENT)
            .build();
    Column[] columns = new Column[] {col1, col2};

    Transform[] partitions =
        new Transform[] {
          day(col2.name()), bucket(10, new String[] {col1.name()}), truncate(2, col1.name())
        };

    Table table =
        icebergCatalogOperations.createTable(
            tableIdentifier, columns, ICEBERG_COMMENT, properties, partitions);
    Assertions.assertEquals(tableIdentifier.name(), table.name());
    Assertions.assertEquals(ICEBERG_COMMENT, table.comment());
    Assertions.assertEquals("val1", table.properties().get("key1"));
    Assertions.assertEquals("val2", table.properties().get("key2"));
    Assertions.assertArrayEquals(partitions, table.partitioning());

    Table loadedTable = icebergCatalogOperations.loadTable(tableIdentifier);

    Assertions.assertEquals("val1", loadedTable.properties().get("key1"));
    Assertions.assertEquals("val2", loadedTable.properties().get("key2"));
    Assertions.assertArrayEquals(partitions, loadedTable.partitioning());

    Assertions.assertTrue(icebergCatalogOperations.tableExists(tableIdentifier));
    NameIdentifier[] tableIdents = icebergCatalogOperations.listTables(tableIdentifier.namespace());
    Assertions.assertTrue(Arrays.asList(tableIdents).contains(tableIdentifier));

    // Test exception
    TableCatalog tableCatalog = icebergCatalogOperations;
    Transform[] partitions1 = new Transform[] {day(col2.name())};
    Throwable exception =
        Assertions.assertThrows(
            TableAlreadyExistsException.class,
            () ->
                tableCatalog.createTable(
                    tableIdentifier, columns, ICEBERG_COMMENT, properties, partitions1));
    Assertions.assertTrue(exception.getMessage().contains("Table already exists"));

    String icebergName = icebergCatalog.name();
    String schemaName = icebergSchema.name();
    String randomName = genRandomName();
    NameIdentifier id = NameIdentifier.of(META_LAKE_NAME, icebergName, schemaName, randomName);
    Transform[] partitions2 = new Transform[] {identity(new String[] {col1.name(), col2.name()})};
    exception =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> tableCatalog.createTable(id, columns, ICEBERG_COMMENT, properties, partitions2));
    Assertions.assertTrue(exception.getMessage().contains("Cannot find source column"));

    Transform[] partitions3 = new Transform[] {identity("not_exist_field")};
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> tableCatalog.createTable(id, columns, ICEBERG_COMMENT, properties, partitions3));
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
        IcebergColumn.builder()
            .withName("col_1")
            .withType(Types.IntegerType.get())
            .withComment(ICEBERG_COMMENT)
            .build();
    IcebergColumn col2 =
        IcebergColumn.builder()
            .withName("col_2")
            .withType(Types.DateType.get())
            .withComment(ICEBERG_COMMENT)
            .build();
    Column[] columns = new Column[] {col1, col2};

    icebergCatalogOperations.createTable(
        tableIdentifier,
        columns,
        ICEBERG_COMMENT,
        properties,
        new Transform[0],
        Distributions.NONE,
        new SortOrder[0]);

    Assertions.assertTrue(icebergCatalogOperations.tableExists(tableIdentifier));
    icebergCatalogOperations.dropTable(tableIdentifier);
    Assertions.assertFalse(icebergCatalogOperations.tableExists(tableIdentifier));
  }

  @Test
  public void testListTableException() {
    Namespace tableNs = Namespace.of("metalake", icebergCatalog.name(), "not_exist_db");
    TableCatalog tableCatalog = icebergCatalogOperations;
    Throwable exception =
        Assertions.assertThrows(
            NoSuchSchemaException.class, () -> tableCatalog.listTables(tableNs));
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
        IcebergColumn.builder()
            .withName("col_1")
            .withType(Types.IntegerType.get())
            .withComment(ICEBERG_COMMENT)
            .build();
    IcebergColumn col2 =
        IcebergColumn.builder()
            .withName("col_2")
            .withType(Types.DateType.get())
            .withComment(ICEBERG_COMMENT)
            .build();
    Column[] columns = new Column[] {col1, col2};

    Distribution distribution = Distributions.NONE;
    SortOrder[] sortOrders = createSortOrder();

    Table createdTable =
        icebergCatalogOperations.createTable(
            tableIdentifier,
            columns,
            ICEBERG_COMMENT,
            properties,
            new Transform[0],
            distribution,
            sortOrders);
    Assertions.assertTrue(icebergCatalogOperations.tableExists(tableIdentifier));

    TableChange update = TableChange.updateComment(ICEBERG_COMMENT + "_new");
    TableChange rename = TableChange.rename("test_iceberg_table_new");
    Throwable exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> icebergCatalogOperations.alterTable(tableIdentifier, update, rename));
    Assertions.assertTrue(
        exception.getMessage().contains("The operation to change the table name cannot"));

    // test alter
    icebergCatalogOperations.alterTable(
        tableIdentifier,
        TableChange.updateComment(ICEBERG_COMMENT + "_new"),
        TableChange.removeProperty("key1"),
        TableChange.setProperty("key2", "val2_new"),
        // columns current format: [col_1:I8:comment, col_2:DATE:comment]
        TableChange.addColumn(new String[] {"col_3"}, Types.StringType.get()),
        // columns current format: [col_1:I8:comment, col_2:DATE:comment, col_3:STRING:null]
        TableChange.renameColumn(new String[] {"col_2"}, "col_2_new"),
        // columns current format: [col_1:I8:comment, col_2_new:DATE:comment, col_3:STRING:null]
        TableChange.updateColumnComment(new String[] {"col_1"}, ICEBERG_COMMENT + "_new"),
        // columns current format: [col_1:I8:comment_new, col_2_new:DATE:comment,
        // col_3:STRING:null]
        TableChange.updateColumnType(new String[] {"col_1"}, Types.IntegerType.get()),
        // columns current format: [col_1:I32:comment_new, col_2_new:DATE:comment,
        // col_3:STRING:null]
        TableChange.updateColumnPosition(new String[] {"col_2"}, TableChange.ColumnPosition.first())
        // columns current: [col_2_new:DATE:comment, col_1:I32:comment_new, col_3:STRING:null]
        );

    icebergCatalogOperations.alterTable(
        tableIdentifier, TableChange.rename("test_iceberg_table_new"));

    Table alteredTable =
        icebergCatalogOperations.loadTable(
            NameIdentifier.of(tableIdentifier.namespace(), "test_iceberg_table_new"));

    Assertions.assertEquals(ICEBERG_COMMENT + "_new", alteredTable.comment());
    Assertions.assertFalse(alteredTable.properties().containsKey("key1"));
    Assertions.assertEquals("val2_new", alteredTable.properties().get("key2"));

    sortOrders[0] =
        SortOrders.of(
            NamedReference.field("col_2_new"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST);
    Assertions.assertEquals(sortOrders.length, alteredTable.sortOrder().length);
    for (int i = 0; i < alteredTable.sortOrder().length; i++) {
      Assertions.assertEquals(sortOrders[i].direction(), alteredTable.sortOrder()[i].direction());
      Assertions.assertEquals(
          (sortOrders[i]).nullOrdering(), alteredTable.sortOrder()[i].nullOrdering());
      Assertions.assertEquals(
          (sortOrders[i]).expression(), alteredTable.sortOrder()[i].expression());
    }

    Column[] expected =
        new Column[] {
          IcebergColumn.builder()
              .withName("col_2_new")
              .withType(Types.DateType.get())
              .withComment(ICEBERG_COMMENT)
              .build(),
          IcebergColumn.builder()
              .withName("col_1")
              .withType(Types.IntegerType.get())
              .withComment(ICEBERG_COMMENT + "_new")
              .build(),
          IcebergColumn.builder()
              .withName("col_3")
              .withType(Types.StringType.get())
              .withComment(null)
              .build()
        };
    Assertions.assertArrayEquals(expected, alteredTable.columns());

    // test delete column change
    icebergCatalogOperations.alterTable(
        NameIdentifier.of(tableIdentifier.namespace(), "test_iceberg_table_new"),
        TableChange.deleteColumn(new String[] {"col_3"}, false));
    Table alteredTable1 =
        icebergCatalogOperations.loadTable(
            NameIdentifier.of(tableIdentifier.namespace(), "test_iceberg_table_new"));
    expected =
        Arrays.stream(expected).filter(c -> !"col_3".equals(c.name())).toArray(Column[]::new);
    Assertions.assertArrayEquals(expected, alteredTable1.columns());

    Assertions.assertNotNull(alteredTable.partitioning());
    Assertions.assertArrayEquals(createdTable.partitioning(), alteredTable.partitioning());
  }

  @Test
  public void testTableProperty() {
    CatalogEntity entity = createDefaultCatalogEntity();
    try (IcebergCatalogOperations ops = new IcebergCatalogOperations()) {
      ops.initialize(
          Maps.newHashMap(),
          entity.toCatalogInfo(),
          TestIcebergCatalog.ICEBERG_PROPERTIES_METADATA);
      Map<String, String> map = Maps.newHashMap();
      map.put(IcebergTablePropertiesMetadata.COMMENT, "test");
      map.put(IcebergTablePropertiesMetadata.CREATOR, "test");
      map.put(IcebergTablePropertiesMetadata.CURRENT_SNAPSHOT_ID, "test");
      map.put(IcebergTablePropertiesMetadata.CHERRY_PICK_SNAPSHOT_ID, "test");
      map.put(IcebergTablePropertiesMetadata.SORT_ORDER, "test");
      map.put(IcebergTablePropertiesMetadata.IDENTIFIER_FIELDS, "test");
      for (Map.Entry<String, String> entry : map.entrySet()) {
        HashMap<String, String> properties =
            new HashMap<String, String>() {
              {
                put(entry.getKey(), entry.getValue());
              }
            };
        PropertiesMetadata metadata = icebergCatalog.tablePropertiesMetadata();
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              PropertiesMetadataHelpers.validatePropertyForCreate(metadata, properties);
            });
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
        PropertiesMetadata metadata = icebergCatalog.tablePropertiesMetadata();
        Assertions.assertDoesNotThrow(
            () -> {
              PropertiesMetadataHelpers.validatePropertyForCreate(metadata, properties);
            });
      }
    }
  }

  @Test
  public void testTableDistribution() {
    IcebergColumn col_1 =
        IcebergColumn.builder()
            .withName("col_1")
            .withType(Types.LongType.get())
            .withComment("test")
            .build();
    IcebergColumn col_2 =
        IcebergColumn.builder()
            .withName("col_2")
            .withType(Types.IntegerType.get())
            .withComment("test2")
            .build();
    List<IcebergColumn> icebergColumns =
        new ArrayList<IcebergColumn>() {
          {
            add(col_1);
            add(col_2);
          }
        };
    IcebergTable icebergTable =
        IcebergTable.builder()
            .withName("test_table")
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .withProperties(Maps.newHashMap())
            .withColumns(icebergColumns.toArray(new IcebergColumn[0]))
            .withComment("test_table")
            .build();
    String none =
        Assertions.assertDoesNotThrow(() -> icebergTable.transformDistribution(Distributions.NONE));
    Assertions.assertEquals(none, DistributionMode.NONE.modeName());

    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> icebergTable.transformDistribution(Distributions.HASH));
    Assertions.assertTrue(
        StringUtils.contains(
            illegalArgumentException.getMessage(),
            "Iceberg's Distribution Mode.HASH is distributed based on partition, but the partition is empty"));

    illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> icebergTable.transformDistribution(Distributions.RANGE));
    Assertions.assertTrue(
        StringUtils.contains(
            illegalArgumentException.getMessage(),
            "Iceberg's Distribution Mode.RANGE is distributed based on sortOrder or partition, but both are empty"));

    IcebergTable newTable =
        IcebergTable.builder()
            .withName("test_table2")
            .withAuditInfo(
                AuditInfo.builder().withCreator("test2").withCreateTime(Instant.now()).build())
            .withProperties(Maps.newHashMap())
            .withPartitioning(new Transform[] {day("col_1")})
            .withSortOrders(
                new SortOrder[] {
                  SortOrders.of(
                      NamedReference.field("col_1"),
                      SortDirection.DESCENDING,
                      NullOrdering.NULLS_FIRST)
                })
            .withColumns(icebergColumns.toArray(new IcebergColumn[0]))
            .withComment("test_table2")
            .build();
    String distributionName =
        Assertions.assertDoesNotThrow(() -> newTable.transformDistribution(Distributions.NONE));
    Assertions.assertEquals(distributionName, DistributionMode.NONE.modeName());
    distributionName =
        Assertions.assertDoesNotThrow(() -> newTable.transformDistribution(Distributions.HASH));
    Assertions.assertEquals(distributionName, DistributionMode.HASH.modeName());
    distributionName =
        Assertions.assertDoesNotThrow(() -> newTable.transformDistribution(Distributions.RANGE));
    Assertions.assertEquals(distributionName, DistributionMode.RANGE.modeName());
  }

  protected static String genRandomName() {
    return UUID.randomUUID().toString().replace("-", "");
  }
}
