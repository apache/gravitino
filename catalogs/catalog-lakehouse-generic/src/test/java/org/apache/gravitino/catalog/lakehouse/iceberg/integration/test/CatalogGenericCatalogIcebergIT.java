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
package org.apache.gravitino.catalog.lakehouse.iceberg.integration.test;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogGenericCatalogIcebergIT extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogGenericCatalogIcebergIT.class);

  public static final String METALAKE_NAME =
      GravitinoITUtils.genRandomName("CatalogGenericLakeIcebergIT_metalake");

  private final String catalogName =
      GravitinoITUtils.genRandomName("CatalogGenericLakeIcebergIT_catalog");
  private final String schemaPrefix = "CatalogGenericLakeIceberg_schema";
  private final String tablePrefix = "CatalogGenericLakeIceberg_table";
  private final String schemaName = GravitinoITUtils.genRandomName(schemaPrefix);
  private final String tableName = GravitinoITUtils.genRandomName(tablePrefix);
  private static final String TABLE_COMMENT = "table_comment";

  private final String provider = "lakehouse-generic";

  private GravitinoMetalake metalake;
  private Catalog catalog;

  @BeforeAll
  public void startup() throws Exception {
    createMetalake();
    createCatalog();
    createSchema();
  }

  @AfterAll
  public void stop() throws IOException {
    if (client != null) {
      Arrays.stream(catalog.asSchemas().listSchemas())
          .filter(schema -> !schema.equals("default"))
          .forEach(schema -> catalog.asSchemas().dropSchema(schema, true));
      Arrays.stream(metalake.listCatalogs()).forEach(name -> metalake.dropCatalog(name, true));
      client.dropMetalake(METALAKE_NAME, true);
    }
    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Failed to close CloseableGroup", e);
    }

    client = null;
  }

  @AfterEach
  public void resetSchema() throws InterruptedException {
    catalog.asSchemas().dropSchema(schemaName, true);
    createSchema();
  }

  @Test
  public void testCreateExternalIcebergTable() {
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Column[] columns = new Column[] {Column.of("id", Types.IntegerType.get(), "id column")};

    Map<String, String> properties = createProperties();
    properties.put(Table.PROPERTY_TABLE_FORMAT, "iceberg");
    properties.put(Table.PROPERTY_EXTERNAL, "true");

    Table createdTable =
        catalog
            .asTableCatalog()
            .createTable(
                tableIdentifier,
                columns,
                TABLE_COMMENT,
                properties,
                Transforms.EMPTY_TRANSFORM,
                null,
                null);

    Assertions.assertEquals(tableName, createdTable.name());
    Assertions.assertEquals(TABLE_COMMENT, createdTable.comment());
    Assertions.assertEquals("iceberg", createdTable.properties().get(Table.PROPERTY_TABLE_FORMAT));
    Assertions.assertEquals("true", createdTable.properties().get(Table.PROPERTY_EXTERNAL));

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> catalog.asTableCatalog().purgeTable(tableIdentifier));
  }

  @Test
  public void testLoadAndAlterExternalIcebergTable() {
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName + "_load_alter");
    Column[] columns =
        new Column[] {
          Column.of("id", Types.IntegerType.get(), "id column"),
          Column.of("name", Types.StringType.get(), "name column")
        };

    Map<String, String> properties = createProperties();
    properties.put(Table.PROPERTY_TABLE_FORMAT, "iceberg");
    properties.put(Table.PROPERTY_EXTERNAL, "true");

    catalog
        .asTableCatalog()
        .createTable(
            tableIdentifier,
            columns,
            TABLE_COMMENT,
            properties,
            Transforms.EMPTY_TRANSFORM,
            null,
            null);

    Table loadedTable = catalog.asTableCatalog().loadTable(tableIdentifier);
    Assertions.assertEquals(tableIdentifier.name(), loadedTable.name());
    Assertions.assertEquals(2, loadedTable.columns().length);
    Assertions.assertEquals("id", loadedTable.columns()[0].name());
    Assertions.assertEquals(Types.IntegerType.get(), loadedTable.columns()[0].dataType());
    Assertions.assertEquals("name", loadedTable.columns()[1].name());
    Assertions.assertEquals(Types.StringType.get(), loadedTable.columns()[1].dataType());
    Assertions.assertEquals("iceberg", loadedTable.properties().get(Table.PROPERTY_TABLE_FORMAT));
    Assertions.assertEquals("true", loadedTable.properties().get(Table.PROPERTY_EXTERNAL));
    Assertions.assertEquals(TABLE_COMMENT, loadedTable.comment());

    Table alteredTable =
        catalog
            .asTableCatalog()
            .alterTable(tableIdentifier, TableChange.updateComment("new_comment"));
    Assertions.assertEquals("new_comment", alteredTable.comment());

    Table reloadedTable = catalog.asTableCatalog().loadTable(tableIdentifier);
    Assertions.assertEquals("new_comment", reloadedTable.comment());
  }

  @Test
  public void testCreateIcebergTableWithoutExternal() {
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName + "_no_external");
    Column[] columns = new Column[] {Column.of("id", Types.IntegerType.get(), "id column")};

    Map<String, String> properties = createProperties();
    properties.put(Table.PROPERTY_TABLE_FORMAT, "iceberg");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog
                .asTableCatalog()
                .createTable(
                    tableIdentifier,
                    columns,
                    TABLE_COMMENT,
                    properties,
                    Transforms.EMPTY_TRANSFORM,
                    null,
                    null));
  }

  protected Map<String, String> createSchemaProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    return properties;
  }

  private void createMetalake() {
    GravitinoMetalake[] metalakes = client.listMetalakes();
    Assertions.assertEquals(0, metalakes.length);

    client.createMetalake(METALAKE_NAME, "comment", Collections.emptyMap());
    GravitinoMetalake loadedMetalake = client.loadMetalake(METALAKE_NAME);
    Assertions.assertEquals(METALAKE_NAME, loadedMetalake.name());

    metalake = loadedMetalake;
  }

  protected void createCatalog() {
    Map<String, String> properties = Maps.newHashMap();
    metalake.createCatalog(catalogName, Catalog.Type.RELATIONAL, provider, "comment", properties);
    catalog = metalake.loadCatalog(catalogName);
  }

  private void createSchema() throws InterruptedException {
    Map<String, String> schemaProperties = createSchemaProperties();
    String comment = "comment";
    catalog.asSchemas().createSchema(schemaName, comment, schemaProperties);
    Schema loadedSchema = catalog.asSchemas().loadSchema(schemaName);
    Assertions.assertEquals(schemaName, loadedSchema.name());
    Assertions.assertEquals(comment, loadedSchema.comment());
    Assertions.assertEquals("val1", loadedSchema.properties().get("key1"));
    Assertions.assertEquals("val2", loadedSchema.properties().get("key2"));
  }

  protected Map<String, String> createProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    return properties;
  }
}
