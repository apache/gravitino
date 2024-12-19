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
package org.apache.gravitino.flink.connector.integration.test.paimon;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Schema;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import org.apache.gravitino.flink.connector.integration.test.FlinkCommonIT;
import org.apache.gravitino.flink.connector.integration.test.utils.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class FlinkPaimonCatalogIT extends FlinkCommonIT {

  private static final String DEFAULT_PAIMON_CATALOG =
      "test_flink_paimon_filesystem_schema_catalog";

  private static org.apache.gravitino.Catalog catalog;

  @Override
  protected boolean supportColumnOperation() {
    return false;
  }

  @Override
  protected boolean supportTableOperation() {
    return false;
  }

  @Override
  protected boolean supportSchemaOperation() {
    return false;
  }

  protected Catalog currentCatalog() {
    return catalog;
  }

  @BeforeAll
  static void setup() {
    initPaimonCatalog();
  }

  @AfterAll
  static void stop() {
    Preconditions.checkNotNull(metalake);
    metalake.dropCatalog(DEFAULT_PAIMON_CATALOG, true);
  }

  private static void initPaimonCatalog() {
    Preconditions.checkNotNull(metalake);
    catalog =
        metalake.createCatalog(
            DEFAULT_PAIMON_CATALOG,
            org.apache.gravitino.Catalog.Type.RELATIONAL,
            "lakehouse-paimon",
            null,
            ImmutableMap.of(
                PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND,
                "filesystem",
                "warehouse",
                "/tmp/gravitino/paimon"));
  }

  @Test
  public void testCreateSchema() {
    doWithCatalog(
        currentCatalog(),
        catalog -> {
          String schema = "test_create_schema";
          try {
            TableResult tableResult = sql("CREATE DATABASE IF NOT EXISTS %s", schema);
            TestUtils.assertTableResult(tableResult, ResultKind.SUCCESS);
            catalog.asSchemas().schemaExists(schema);
          } finally {
            catalog.asSchemas().dropSchema(schema, true);
            Assertions.assertFalse(catalog.asSchemas().schemaExists(schema));
          }
        });
  }

  @Test
  public void testGetSchema() {
    doWithCatalog(
        currentCatalog(),
        catalog -> {
          String schema = "test_get_schema";
          try {
            TestUtils.assertTableResult(
                sql("CREATE DATABASE IF NOT EXISTS %s", schema), ResultKind.SUCCESS);
            TestUtils.assertTableResult(tableEnv.executeSql("USE " + schema), ResultKind.SUCCESS);

            catalog.asSchemas().schemaExists(schema);
            Schema loadedSchema = catalog.asSchemas().loadSchema(schema);
            Assertions.assertEquals(schema, loadedSchema.name());
          } finally {
            catalog.asSchemas().dropSchema(schema, true);
            Assertions.assertFalse(catalog.asSchemas().schemaExists(schema));
          }
        });
  }

  @Test
  public void testListSchema() {
    doWithCatalog(
        currentCatalog(),
        catalog -> {
          String schema = "test_list_schema";
          String schema2 = "test_list_schema2";
          String schema3 = "test_list_schema3";

          try {
            TestUtils.assertTableResult(
                sql("CREATE DATABASE IF NOT EXISTS %s", schema), ResultKind.SUCCESS);
            TestUtils.assertTableResult(
                sql("CREATE DATABASE IF NOT EXISTS %s", schema2), ResultKind.SUCCESS);
            TestUtils.assertTableResult(
                sql("CREATE DATABASE IF NOT EXISTS %s", schema3), ResultKind.SUCCESS);
            TestUtils.assertTableResult(
                sql("SHOW DATABASES"),
                ResultKind.SUCCESS_WITH_CONTENT,
                Row.of("default"),
                Row.of(schema),
                Row.of(schema2),
                Row.of(schema3));

            String[] schemas = catalog.asSchemas().listSchemas();
            Arrays.sort(schemas);
            Assertions.assertEquals(4, schemas.length);
            Assertions.assertEquals("default", schemas[0]);
            Assertions.assertEquals(schema, schemas[1]);
            Assertions.assertEquals(schema2, schemas[2]);
            Assertions.assertEquals(schema3, schemas[3]);
          } finally {
            catalog.asSchemas().dropSchema(schema, true);
            catalog.asSchemas().dropSchema(schema2, true);
            catalog.asSchemas().dropSchema(schema3, true);
            Assertions.assertEquals(1, catalog.asSchemas().listSchemas().length);
          }
        });
  }

  @Test
  public void testCreateGravitinoPaimonCatalogUsingSQL() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    int numCatalogs = tableEnv.listCatalogs().length;
    String catalogName = "gravitino_hive_sql";
    String warehouse = "/tmp/gravitino/paimon";
    tableEnv.executeSql(
        String.format(
            "create catalog %s with ("
                + "'type'='gravitino-paimon', "
                + "'warehouse'='%s',"
                + "'catalog.backend'='filesystem'"
                + ")",
            catalogName, warehouse));
    String[] catalogs = tableEnv.listCatalogs();
    Assertions.assertEquals(numCatalogs + 1, catalogs.length, "Should create a new catalog");
    Assertions.assertTrue(metalake.catalogExists(catalogName));
    org.apache.gravitino.Catalog gravitinoCatalog = metalake.loadCatalog(catalogName);
    Map<String, String> properties = gravitinoCatalog.properties();
    Assertions.assertEquals(warehouse, properties.get("warehouse"));
  }
}
