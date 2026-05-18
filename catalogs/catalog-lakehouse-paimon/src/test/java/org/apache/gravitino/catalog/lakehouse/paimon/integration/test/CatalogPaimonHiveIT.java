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
package org.apache.gravitino.catalog.lakehouse.paimon.integration.test;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewCatalog;
import org.apache.gravitino.rel.types.Types;
import org.apache.paimon.catalog.Catalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalogPaimonHiveIT extends CatalogPaimonBaseIT {

  @Override
  protected Map<String, String> initPaimonCatalogProperties() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put("key1", "val1");
    catalogProperties.put("key2", "val2");

    TYPE = "hive";
    WAREHOUSE =
        String.format(
            "hdfs://%s:%d/user/hive/warehouse-catalog-paimon/",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT);
    URI =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    catalogProperties.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, TYPE);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, WAREHOUSE);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.URI, URI);

    return catalogProperties;
  }

  @Override
  protected void initSparkEnv() {
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("Paimon Catalog integration test")
            .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
            .config("spark.sql.catalog.paimon.metastore", "hive")
            .config("spark.sql.catalog.paimon.uri", URI)
            .config("spark.sql.catalog.paimon.warehouse", WAREHOUSE)
            .config("spark.sql.catalog.paimon.cache-enabled", "false")
            .config(
                "spark.sql.extensions",
                "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
            .enableHiveSupport()
            .getOrCreate();
  }

  @Test
  @Override
  void testSparkCreateViewAndLoadByGravitino() {
    NameIdentifier baseTableIdentifier =
        createSimplePaimonTableForViewInterop("spark_create_view_source_table");
    String viewName = GravitinoITUtils.genRandomName("spark_create_view");
    String viewIdentifier = String.join(".", schemaName, viewName);
    String tableIdentifier = String.join(".", schemaName, baseTableIdentifier.name());

    ViewCatalog viewCatalog = catalog.asViewCatalog();
    spark.sql(
        String.format(
            "CREATE VIEW paimon.%s AS SELECT id, name FROM paimon.%s",
            viewIdentifier, tableIdentifier));

    View loadedView = viewCatalog.loadView(NameIdentifier.of(schemaName, viewName));
    Assertions.assertEquals(viewName, loadedView.name());
    Assertions.assertEquals(2, loadedView.columns().length);
    Assertions.assertEquals("id", loadedView.columns()[0].name());
    Assertions.assertEquals("name", loadedView.columns()[1].name());
    Assertions.assertTrue(loadedView.representations().length > 0);
  }

  @Test
  @Override
  void testGravitinoCreateViewAndReadBySpark() {
    NameIdentifier baseTableIdentifier =
        createSimplePaimonTableForViewInterop("gravitino_create_view_source_table");
    String viewName = GravitinoITUtils.genRandomName("gravitino_create_view");
    NameIdentifier viewIdentifier = NameIdentifier.of(schemaName, viewName);

    String query =
        String.format("SELECT id, name FROM paimon.%s.%s", schemaName, baseTableIdentifier.name());
    Representation[] representations =
        new Representation[] {
          SQLRepresentation.builder().withDialect("query").withSql(query).build(),
          SQLRepresentation.builder().withDialect("spark").withSql(query).build()
        };
    ViewCatalog viewCatalog = catalog.asViewCatalog();

    viewCatalog.createView(
        viewIdentifier,
        "view_for_spark_read",
        new Column[] {
          Column.of("id", Types.IntegerType.get(), "id column"),
          Column.of("name", Types.StringType.get(), "name column")
        },
        representations,
        "paimon",
        schemaName,
        Collections.emptyMap());

    Dataset<Row> rows =
        spark.sql(String.format("SELECT * FROM paimon.%s.%s ORDER BY id", schemaName, viewName));
    List<Row> results = rows.collectAsList();
    Assertions.assertEquals(2, results.size());
    Assertions.assertEquals(1, results.get(0).getInt(0));
    Assertions.assertEquals("name_1", results.get(0).getString(1));
    Assertions.assertEquals(2, results.get(1).getInt(0));
    Assertions.assertEquals("name_2", results.get(1).getString(1));
  }

  @Test
  void testPaimonSchemaProperties() throws Catalog.DatabaseNotExistException {
    SupportsSchemas schemas = catalog.asSchemas();

    // create schema check.
    String testSchemaName = GravitinoITUtils.genRandomName("test_schema_1");
    NameIdentifier schemaIdent = NameIdentifier.of(metalakeName, catalogName, testSchemaName);
    Map<String, String> schemaProperties = Maps.newHashMap();
    schemaProperties.put("key", "hive");
    Schema createdSchema =
        schemas.createSchema(schemaIdent.name(), schema_comment, schemaProperties);
    Assertions.assertEquals(createdSchema.properties().get("key"), "hive");

    // load schema check.
    Schema schema = schemas.loadSchema(schemaIdent.name());
    Assertions.assertEquals(schema.properties().get("key"), "hive");
    Map<String, String> loadedProps = paimonCatalog.getDatabase(schemaIdent.name()).options();
    Assertions.assertEquals(loadedProps.get("key"), "hive");
  }
}
