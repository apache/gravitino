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
package org.apache.gravitino.flink.connector.paimon;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;
import org.apache.gravitino.flink.connector.CatalogPropertiesConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test for {@link PaimonPropertiesConverter} */
public class TestPaimonPropertiesConverter {

  private static final PaimonPropertiesConverter CONVERTER = PaimonPropertiesConverter.INSTANCE;

  private static final String LOCAL_WAREHOUSE = "file:///tmp/paimon_warehouse";

  @Test
  public void testToPaimonFileSystemCatalog() {
    Map<String, String> catalogProperties = ImmutableMap.of("warehouse", LOCAL_WAREHOUSE);
    Map<String, String> flinkCatalogProperties =
        CONVERTER.toFlinkCatalogProperties(catalogProperties);
    Assertions.assertEquals(
        GravitinoPaimonCatalogFactoryOptions.IDENTIFIER, flinkCatalogProperties.get("type"));
    Assertions.assertEquals(LOCAL_WAREHOUSE, flinkCatalogProperties.get("warehouse"));
  }

  @Test
  public void testToPaimonJdbcCatalog() {
    String testUser = "testUser";
    String testPassword = "testPassword";
    String testUri = "testUri";
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            PaimonConstants.WAREHOUSE,
            LOCAL_WAREHOUSE,
            PaimonConstants.CATALOG_BACKEND,
            "jdbc",
            PaimonConstants.GRAVITINO_JDBC_USER,
            testUser,
            PaimonConstants.GRAVITINO_JDBC_PASSWORD,
            testPassword,
            CatalogPropertiesConverter.FLINK_PROPERTY_PREFIX + PaimonConstants.URI,
            testUri);
    Map<String, String> flinkCatalogProperties =
        CONVERTER.toFlinkCatalogProperties(catalogProperties);
    Assertions.assertEquals(
        GravitinoPaimonCatalogFactoryOptions.IDENTIFIER, flinkCatalogProperties.get("type"));
    Assertions.assertEquals(LOCAL_WAREHOUSE, flinkCatalogProperties.get(PaimonConstants.WAREHOUSE));
    Assertions.assertEquals(testUser, flinkCatalogProperties.get(PaimonConstants.PAIMON_JDBC_USER));
    Assertions.assertEquals(
        testPassword, flinkCatalogProperties.get(PaimonConstants.PAIMON_JDBC_PASSWORD));
    Assertions.assertEquals("jdbc", flinkCatalogProperties.get(PaimonConstants.METASTORE));
    Assertions.assertEquals(testUri, flinkCatalogProperties.get(PaimonConstants.URI));
  }

  // -----------------------------------------------------------------------
  // toFlinkTableProperties — catalog options merged for HiveMetastoreClient
  // -----------------------------------------------------------------------

  @Test
  public void testToFlinkTablePropertiesMergesCatalogBackendAsMetastore() {
    Map<String, String> catalogOpts =
        ImmutableMap.of(
            PaimonConstants.CATALOG_BACKEND,
            "hive",
            PaimonConstants.URI,
            "thrift://localhost:9083",
            PaimonConstants.WAREHOUSE,
            "hdfs://warehouse",
            PaimonConstants.CATALOG_BACKEND_NAME,
            "hive_catalog");
    Map<String, String> tableOpts = ImmutableMap.of("primary-key", "id");
    ObjectPath path = new ObjectPath("db", "tbl");

    Map<String, String> result = CONVERTER.toFlinkTableProperties(catalogOpts, tableOpts, path);

    // catalog-backend=hive must be translated and injected as metastore=hive
    Assertions.assertEquals("hive", result.get(PaimonConstants.METASTORE));
    Assertions.assertEquals("thrift://localhost:9083", result.get(PaimonConstants.URI));
    Assertions.assertEquals("hdfs://warehouse", result.get(PaimonConstants.WAREHOUSE));
    Assertions.assertEquals("hive_catalog", result.get(PaimonConstants.CATALOG_BACKEND_NAME));
    // table-level property must be preserved
    Assertions.assertEquals("id", result.get("primary-key"));
  }

  /**
   * When the catalog was constructed with Paimon-native keys (e.g. {@code metastore=hive} rather
   * than the Gravitino-side {@code catalog-backend=hive}), the key must be recognised and
   * propagated as-is so that {@link org.apache.paimon.flink.FlinkTableFactory} can still create the
   * HiveMetastoreClient during data-write commits.
   */
  @Test
  public void testToFlinkTablePropertiesHandlesPaimonNativeMetastoreKey() {
    // Catalog options already use the Paimon-native "metastore" key.
    Map<String, String> catalogOpts =
        ImmutableMap.of(
            PaimonConstants.METASTORE, "hive", PaimonConstants.WAREHOUSE, LOCAL_WAREHOUSE);
    Map<String, String> tableOpts = ImmutableMap.of("primary-key", "id");
    ObjectPath path = new ObjectPath("db", "tbl");

    Map<String, String> result = CONVERTER.toFlinkTableProperties(catalogOpts, tableOpts, path);

    // The native "metastore=hive" key must be kept as-is in the table options.
    Assertions.assertEquals("hive", result.get(PaimonConstants.METASTORE));
    Assertions.assertEquals(LOCAL_WAREHOUSE, result.get(PaimonConstants.WAREHOUSE));
    Assertions.assertEquals("id", result.get("primary-key"));
  }

  @Test
  public void testToFlinkTablePropertiesTableLevelWins() {
    // If the table already has metastore set, catalog-level must NOT override it.
    Map<String, String> catalogOpts = ImmutableMap.of(PaimonConstants.CATALOG_BACKEND, "hive");
    Map<String, String> tableOpts = ImmutableMap.of(PaimonConstants.METASTORE, "filesystem");
    ObjectPath path = new ObjectPath("db", "tbl");

    Map<String, String> result = CONVERTER.toFlinkTableProperties(catalogOpts, tableOpts, path);

    Assertions.assertEquals("filesystem", result.get(PaimonConstants.METASTORE));
  }

  @Test
  public void testToFlinkTablePropertiesIgnoresUnknownCatalogKeys() {
    Map<String, String> catalogOpts =
        ImmutableMap.of(
            "type",
            GravitinoPaimonCatalogFactoryOptions.IDENTIFIER,
            "gravitino.uri",
            "http://localhost:8090",
            PaimonConstants.WAREHOUSE,
            LOCAL_WAREHOUSE);
    Map<String, String> tableOpts = Collections.emptyMap();
    ObjectPath path = new ObjectPath("db", "tbl");

    Map<String, String> result = CONVERTER.toFlinkTableProperties(catalogOpts, tableOpts, path);

    // Gravitino-specific / Flink-specific keys without a Paimon mapping must be dropped
    Assertions.assertFalse(result.containsKey("type"));
    Assertions.assertFalse(result.containsKey("gravitino.uri"));
    // warehouse has a known mapping and must be present
    Assertions.assertEquals(LOCAL_WAREHOUSE, result.get(PaimonConstants.WAREHOUSE));
  }

  @Test
  public void testToGravitinoCatalogProperties() {
    String testUser = "testUser";
    String testPassword = "testPassword";
    String testUri = "testUri";
    String testBackend = "jdbc";
    Configuration configuration =
        Configuration.fromMap(
            ImmutableMap.of(
                PaimonConstants.WAREHOUSE,
                LOCAL_WAREHOUSE,
                PaimonConstants.METASTORE,
                testBackend,
                PaimonConstants.PAIMON_JDBC_USER,
                testUser,
                PaimonConstants.PAIMON_JDBC_PASSWORD,
                testPassword,
                PaimonConstants.URI,
                testUri));
    Map<String, String> properties = CONVERTER.toGravitinoCatalogProperties(configuration);
    Assertions.assertEquals(LOCAL_WAREHOUSE, properties.get(PaimonConstants.WAREHOUSE));
    Assertions.assertEquals(testUser, properties.get(PaimonConstants.GRAVITINO_JDBC_USER));
    Assertions.assertEquals(testPassword, properties.get(PaimonConstants.GRAVITINO_JDBC_PASSWORD));
    Assertions.assertEquals(testUri, properties.get(PaimonConstants.URI));
    Assertions.assertEquals(testBackend, properties.get(PaimonConstants.CATALOG_BACKEND));
  }
}
