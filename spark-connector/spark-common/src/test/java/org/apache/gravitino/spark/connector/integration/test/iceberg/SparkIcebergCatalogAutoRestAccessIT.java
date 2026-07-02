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
package org.apache.gravitino.spark.connector.integration.test.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.iceberg.IcebergPropertiesConstants;
import org.apache.gravitino.spark.connector.integration.test.SparkEnvIT;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

/**
 * Integration tests for automatic Iceberg REST catalog registration via {@code
 * spark.sql.gravitino.iceberg.enableRestAccess=true}.
 *
 * <p>Setup:
 *
 * <ul>
 *   <li>The Gravitino catalog is a standard {@code lakehouse-iceberg} catalog backed by Hive.
 *   <li>The Gravitino auxiliary Iceberg REST service is started with {@code
 *       dynamic-config-provider}, so it loads catalog configuration from Gravitino at request time
 *       and exposes the catalog under its Gravitino name.
 *   <li>Spark is configured with {@code spark.sql.gravitino.iceberg.enableRestAccess=true} (and
 *       {@code enableIcebergSupport} disabled), so the Gravitino plugin registers a native {@code
 *       org.apache.iceberg.spark.SparkCatalog} with {@code type=rest} for each Iceberg catalog.
 * </ul>
 *
 * <p>These tests verify the auto-registration feature itself (catalog is present, basic DDL/DML
 * works through the native REST catalog). They do not inherit from {@link SparkIcebergCatalogIT}
 * because that class assumes a Gravitino wrapper catalog, which is incompatible with the native
 * Iceberg {@code SparkTable} returned by a REST catalog.
 */
@Tag("gravitino-docker-test")
// The native Iceberg REST catalog client shipped with the Spark connector runtime uses a lower
// Iceberg version than the Gravitino REST server, which can cause incompatibilities in embedded
// mode.  The non-embedded (standalone Gravitino + Docker Hive) path works correctly.
@DisabledIf("org.apache.gravitino.integration.test.util.ITUtils#isEmbedded")
public abstract class SparkIcebergCatalogAutoRestAccessIT extends SparkEnvIT {

  private static final String GRAVITINO_ICEBERG_REST_PREFIX = "gravitino.iceberg-rest.";
  private static final String TEST_DB = "rest_auto_test_db";
  private static final String TEST_TABLE = "rest_auto_test_tbl";

  @Override
  public String getCatalogName() {
    return "iceberg";
  }

  @Override
  public String getProvider() {
    return "lakehouse-iceberg";
  }

  @Override
  protected boolean supportsFunction() {
    return false;
  }

  /**
   * Returns catalog properties for a standard Hive-backed Iceberg catalog. The Gravitino auxiliary
   * Iceberg REST service (configured with dynamic-config-provider) will pick up this configuration
   * automatically and expose it under the Gravitino catalog name.
   */
  @Override
  protected Map<String, String> getCatalogConfigs() {
    Map<String, String> props = Maps.newHashMap();
    props.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_HIVE);
    props.put(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI, hiveMetastoreUri);
    props.put(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE, warehouse);
    return props;
  }

  /**
   * Uses {@code dynamic-config-provider} so the Iceberg REST service loads catalog configuration
   * from Gravitino at request time and routes requests by catalog name (warehouse parameter).
   */
  @Override
  protected void initIcebergRestServiceEnv() {
    super.ignoreIcebergAuxRestService = false;
    registerCustomConfigs(
        ImmutableMap.of(
            GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.ICEBERG_REST_CATALOG_CONFIG_PROVIDER,
            IcebergConstants.DYNAMIC_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME,
            GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_METALAKE,
            "test",
            GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_SIMPLE_USERNAME,
            "iceberg-rest-server"));
  }

  /**
   * Disables the Gravitino wrapper path ({@code enableIcebergSupport=false}) and enables native
   * REST access ({@code enableRestAccess=true}).
   */
  @Override
  protected Map<String, String> getExtraSparkConfigs() {
    return ImmutableMap.of(
        GravitinoSparkConfig.GRAVITINO_ENABLE_ICEBERG_SUPPORT, "false",
        GravitinoSparkConfig.GRAVITINO_ICEBERG_ENABLE_REST_ACCESS, "true");
  }

  @BeforeEach
  void createTestDatabase() {
    sql(String.format("CREATE DATABASE IF NOT EXISTS %s.%s", getCatalogName(), TEST_DB));
  }

  @AfterEach
  void dropTestDatabase() {
    sql(String.format("DROP TABLE IF EXISTS %s.%s.%s", getCatalogName(), TEST_DB, TEST_TABLE));
    sql(String.format("DROP DATABASE IF EXISTS %s.%s", getCatalogName(), TEST_DB));
  }

  @Test
  void testCatalogIsRegisteredAsRestCatalog() {
    Set<String> catalogs = getCatalogs();
    Assertions.assertTrue(
        catalogs.contains(getCatalogName()),
        "Expected catalog '" + getCatalogName() + "' to be registered, found: " + catalogs);
  }

  @Test
  void testCreateTableInsertAndSelect() {
    String fullTable = String.format("%s.%s.%s", getCatalogName(), TEST_DB, TEST_TABLE);
    sql(String.format("CREATE TABLE %s (id INT, name STRING) USING iceberg", fullTable));
    sql(String.format("INSERT INTO %s VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')", fullTable));
    List<String> rows =
        getQueryData(String.format("SELECT id, name FROM %s ORDER BY id", fullTable));
    Assertions.assertEquals(3, rows.size());
    Assertions.assertEquals("1,alice", rows.get(0));
    Assertions.assertEquals("2,bob", rows.get(1));
    Assertions.assertEquals("3,carol", rows.get(2));
  }

  @Test
  void testShowTablesInDatabase() {
    String fullTable = String.format("%s.%s.%s", getCatalogName(), TEST_DB, TEST_TABLE);
    sql(String.format("CREATE TABLE %s (id INT) USING iceberg", fullTable));
    Set<String> tables = listTableNames(getCatalogName() + "." + TEST_DB);
    Assertions.assertTrue(
        tables.contains(TEST_TABLE),
        "Expected table '" + TEST_TABLE + "' in SHOW TABLES, found: " + tables);
  }

  @Test
  void testDropAndRecreateTable() {
    String fullTable = String.format("%s.%s.%s", getCatalogName(), TEST_DB, TEST_TABLE);
    sql(String.format("CREATE TABLE %s (id INT) USING iceberg", fullTable));
    sql(String.format("INSERT INTO %s VALUES (42)", fullTable));
    sql(String.format("DROP TABLE %s", fullTable));
    // Re-create with different schema to verify the table is truly gone.
    sql(String.format("CREATE TABLE %s (id INT, val STRING) USING iceberg", fullTable));
    sql(String.format("INSERT INTO %s VALUES (1, 'x')", fullTable));
    List<String> rows = getQueryData(String.format("SELECT id, val FROM %s", fullTable));
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("1,x", rows.get(0));
  }

  @Test
  void testPartitionedTable() {
    String fullTable = String.format("%s.%s.%s", getCatalogName(), TEST_DB, TEST_TABLE);
    sql(
        String.format(
            "CREATE TABLE %s (id INT, category STRING) USING iceberg"
                + " PARTITIONED BY (category)",
            fullTable));
    sql(String.format("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'a')", fullTable));
    List<String> all = getQueryData(String.format("SELECT id FROM %s ORDER BY id", fullTable));
    Assertions.assertEquals(3, all.size());
    List<String> filtered =
        getQueryData(
            String.format("SELECT id FROM %s WHERE category = 'a' ORDER BY id", fullTable));
    Assertions.assertEquals(2, filtered.size());
    Assertions.assertEquals("1", filtered.get(0));
    Assertions.assertEquals("3", filtered.get(1));
  }
}
