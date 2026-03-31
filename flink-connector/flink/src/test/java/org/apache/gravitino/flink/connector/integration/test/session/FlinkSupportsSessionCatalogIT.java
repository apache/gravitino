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
package org.apache.gravitino.flink.connector.integration.test.session;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.flink.connector.hive.GravitinoHiveCatalogFactoryOptions;
import org.apache.gravitino.flink.connector.integration.test.FlinkEnvIT;
import org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests verifying that the {@code gravitino.supportSessionCatalog} option correctly
 * routes catalog operations to the appropriate store:
 *
 * <ul>
 *   <li>Gravitino-managed catalog types (e.g. {@code gravitino-hive}) are persisted to the
 *       Gravitino server via {@link
 *       org.apache.gravitino.flink.connector.store.GravitinoCatalogStore}.
 *   <li>Session-scoped catalog types (e.g. {@code generic_in_memory}) are held in memory only and
 *       never reach the Gravitino server.
 *   <li>Both stores contribute to the catalog list returned by {@code SHOW CATALOGS}.
 * </ul>
 *
 * <p>Requires a running Hive Docker container for the Gravitino-managed catalog tests.
 */
@Tag("gravitino-docker-test")
public class FlinkSupportSessionCatalogIT extends FlinkEnvIT {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkSupportSessionCatalogIT.class);

  private static String hiveConfDir;
  private static Path hiveConfDirPath;

  @Override
  protected String getProvider() {
    return "hive";
  }

  /**
   * Overrides the default Flink environment to enable {@code gravitino.supportSessionCatalog=true},
   * which wires a {@link org.apache.gravitino.flink.connector.store.GravitinoSessionCatalogStore}
   * as the catalog store.
   */
  @Override
  protected void initFlinkEnv() {
    initHiveConfDir();
    final Configuration configuration = new Configuration();
    configuration.setString(
        "table.catalog-store.kind", GravitinoCatalogStoreFactoryOptions.GRAVITINO);
    configuration.setString("table.catalog-store.gravitino.gravitino.metalake", GRAVITINO_METALAKE);
    configuration.setString("table.catalog-store.gravitino.gravitino.uri", gravitinoUri);
    configuration.setBoolean("table.catalog-store.gravitino.gravitino.supportSessionCatalog", true);
    EnvironmentSettings settings =
        EnvironmentSettings.newInstance().withConfiguration(configuration).inBatchMode().build();
    tableEnv = TableEnvironment.create(settings);
    LOG.info(
        "Flink env with supportSessionCatalog=true initialized, Gravitino uri: {}.", gravitinoUri);
  }

  @BeforeAll
  void sessionCatalogStartUp() {
    Preconditions.checkArgument(metalake != null, "metalake should not be null");
    LOG.info("FlinkSupportSessionCatalogIT startup complete.");
  }

  @AfterAll
  static void sessionCatalogStop() {
    Preconditions.checkArgument(metalake != null, "metalake should not be null");
    deleteHiveConfDir();
    LOG.info("FlinkSupportSessionCatalogIT teardown complete.");
  }

  /**
   * A Gravitino-managed catalog type ({@code gravitino-hive}) created via {@link
   * TableEnvironment#createCatalog} must be persisted to the Gravitino server.
   */
  @Test
  public void testCreateGravitinoHiveCatalog() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    int numCatalogs = tableEnv.listCatalogs().length;
    String catalogName = "session_it_gravitino_hive";

    Configuration conf = new Configuration();
    conf.set(CommonCatalogOptions.CATALOG_TYPE, GravitinoHiveCatalogFactoryOptions.IDENTIFIER);
    conf.set(HiveCatalogFactoryOptions.HIVE_CONF_DIR, hiveConfDir);
    conf.set(GravitinoHiveCatalogFactoryOptions.HIVE_METASTORE_URIS, hiveMetastoreUri);
    CatalogDescriptor descriptor = CatalogDescriptor.of(catalogName, conf);
    tableEnv.createCatalog(catalogName, descriptor);

    try {
      Assertions.assertTrue(
          metalake.catalogExists(catalogName),
          "Gravitino-managed catalog must be persisted to the Gravitino server");
      String[] catalogs = tableEnv.listCatalogs();
      Assertions.assertEquals(numCatalogs + 1, catalogs.length, "Should create a new catalog");
      Assertions.assertTrue(
          Arrays.asList(catalogs).contains(catalogName), "Should contain the created catalog");
      org.apache.gravitino.Catalog gravitinoCatalog = metalake.loadCatalog(catalogName);
      Assertions.assertEquals(
          hiveMetastoreUri, gravitinoCatalog.properties().get(HiveConstants.METASTORE_URIS));
    } finally {
      tableEnv.useCatalog(DEFAULT_CATALOG);
      tableEnv.executeSql("DROP CATALOG " + catalogName);
      Assertions.assertFalse(metalake.catalogExists(catalogName));
    }
  }

  /**
   * A Gravitino-managed catalog type ({@code gravitino-hive}) created via Flink SQL {@code CREATE
   * CATALOG} must be persisted to the Gravitino server.
   */
  @Test
  public void testCreateGravitinoHiveCatalogUsingSQL() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    int numCatalogs = tableEnv.listCatalogs().length;
    String catalogName = "session_it_gravitino_hive_sql";

    tableEnv.executeSql(
        String.format(
            "CREATE CATALOG %s WITH ("
                + "'type'='%s',"
                + "'hive-conf-dir'='%s',"
                + "'hive.metastore.uris'='%s'"
                + ")",
            catalogName,
            GravitinoHiveCatalogFactoryOptions.IDENTIFIER,
            hiveConfDir,
            hiveMetastoreUri));

    try {
      Assertions.assertTrue(
          metalake.catalogExists(catalogName),
          "Gravitino-managed catalog must be persisted to the Gravitino server");
      String[] catalogs = tableEnv.listCatalogs();
      Assertions.assertEquals(numCatalogs + 1, catalogs.length, "Should create a new catalog");
      Assertions.assertTrue(
          Arrays.asList(catalogs).contains(catalogName), "Should contain the created catalog");
    } finally {
      tableEnv.useCatalog(DEFAULT_CATALOG);
      tableEnv.executeSql("DROP CATALOG " + catalogName);
      Assertions.assertFalse(metalake.catalogExists(catalogName));
    }
  }

  /**
   * A session-scoped catalog type ({@code generic_in_memory}) created via Flink SQL must be
   * accessible in Flink but must NOT be persisted to the Gravitino server.
   */
  @Test
  public void testCreateSessionScopedCatalog() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    int numCatalogs = tableEnv.listCatalogs().length;
    String catalogName = "session_it_memory_catalog";

    tableEnv.executeSql(
        String.format("CREATE CATALOG %s WITH ('type'='generic_in_memory')", catalogName));

    try {
      Assertions.assertFalse(
          metalake.catalogExists(catalogName),
          "Session-scoped catalog must NOT be persisted to the Gravitino server");
      String[] catalogs = tableEnv.listCatalogs();
      Assertions.assertEquals(numCatalogs + 1, catalogs.length, "Should create a new catalog");
      Assertions.assertTrue(
          Arrays.asList(catalogs).contains(catalogName), "Should contain the created catalog");
    } finally {
      tableEnv.useCatalog(DEFAULT_CATALOG);
      tableEnv.executeSql("DROP CATALOG " + catalogName);
    }
  }

  /**
   * {@code SHOW CATALOGS} must return catalogs from both the Gravitino-backed store and the
   * in-memory store when {@code supportSessionCatalog=true}.
   */
  @Test
  public void testListCatalogsReturnsBothStores() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    int numCatalogs = tableEnv.listCatalogs().length;
    String gravitinoCatalogName = "session_it_list_gravitino_hive";
    String sessionCatalogName = "session_it_list_memory_catalog";

    tableEnv.executeSql(
        String.format(
            "CREATE CATALOG %s WITH ("
                + "'type'='%s',"
                + "'hive-conf-dir'='%s',"
                + "'hive.metastore.uris'='%s'"
                + ")",
            gravitinoCatalogName,
            GravitinoHiveCatalogFactoryOptions.IDENTIFIER,
            hiveConfDir,
            hiveMetastoreUri));
    tableEnv.executeSql(
        String.format("CREATE CATALOG %s WITH ('type'='generic_in_memory')", sessionCatalogName));

    try {
      String[] catalogs = tableEnv.listCatalogs();
      Assertions.assertEquals(numCatalogs + 2, catalogs.length, "Should have two more catalogs");
      Assertions.assertTrue(
          Arrays.asList(catalogs).contains(gravitinoCatalogName),
          "Gravitino-managed catalog must appear in SHOW CATALOGS");
      Assertions.assertTrue(
          Arrays.asList(catalogs).contains(sessionCatalogName),
          "Session-scoped catalog must appear in SHOW CATALOGS");
    } finally {
      tableEnv.useCatalog(DEFAULT_CATALOG);
      tableEnv.executeSql("DROP CATALOG " + gravitinoCatalogName);
      tableEnv.executeSql("DROP CATALOG " + sessionCatalogName);
      Assertions.assertFalse(metalake.catalogExists(gravitinoCatalogName));
    }
  }

  /**
   * Dropping a Gravitino-managed catalog via Flink SQL must remove it from the Gravitino server and
   * from the Flink catalog list.
   */
  @Test
  public void testDropGravitinoHiveCatalog() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    String catalogName = "session_it_drop_gravitino_hive";

    tableEnv.executeSql(
        String.format(
            "CREATE CATALOG %s WITH ("
                + "'type'='%s',"
                + "'hive-conf-dir'='%s',"
                + "'hive.metastore.uris'='%s'"
                + ")",
            catalogName,
            GravitinoHiveCatalogFactoryOptions.IDENTIFIER,
            hiveConfDir,
            hiveMetastoreUri));
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    tableEnv.executeSql("DROP CATALOG " + catalogName);

    Assertions.assertFalse(
        metalake.catalogExists(catalogName),
        "Dropped Gravitino-managed catalog must be removed from the Gravitino server");
    Optional<Catalog> droppedCatalog = tableEnv.getCatalog(catalogName);
    Assertions.assertFalse(droppedCatalog.isPresent(), "Catalog should be dropped");
  }

  /**
   * Dropping a session-scoped catalog via Flink SQL must remove it from the in-memory store without
   * error, since it was never registered in Gravitino.
   */
  @Test
  public void testDropSessionScopedCatalog() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    String catalogName = "session_it_drop_memory_catalog";

    tableEnv.executeSql(
        String.format("CREATE CATALOG %s WITH ('type'='generic_in_memory')", catalogName));
    Assertions.assertFalse(
        metalake.catalogExists(catalogName),
        "Session-scoped catalog must NOT be in Gravitino before drop");

    tableEnv.executeSql("DROP CATALOG " + catalogName);

    Optional<Catalog> droppedCatalog = tableEnv.getCatalog(catalogName);
    Assertions.assertFalse(droppedCatalog.isPresent(), "Catalog should be dropped");
  }

  private static void initHiveConfDir() {
    if (hiveConfDir != null) {
      return;
    }
    try {
      hiveConfDirPath = Files.createTempDirectory("flink-session-hive-conf");
      Path hiveSite = hiveConfDirPath.resolve("hive-site.xml");
      String hiveSiteXml =
          "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
              + "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n"
              + "<configuration>\n"
              + "  <property>\n"
              + "    <name>hive.metastore.sasl.enabled</name>\n"
              + "    <value>false</value>\n"
              + "  </property>\n"
              + "  <property>\n"
              + "    <name>hive.metastore.uris</name>\n"
              + "    <value>"
              + hiveMetastoreUri
              + "</value>\n"
              + "  </property>\n"
              + "  <property>\n"
              + "    <name>hadoop.security.authentication</name>\n"
              + "    <value>simple</value>\n"
              + "  </property>\n"
              + "  <property>\n"
              + "    <name>hive.metastore.warehouse.dir</name>\n"
              + "    <value>"
              + warehouse
              + "</value>\n"
              + "  </property>\n"
              + "</configuration>\n";
      Files.write(hiveSite, hiveSiteXml.getBytes(StandardCharsets.UTF_8));
      hiveConfDir = hiveConfDirPath.toAbsolutePath().toString();
    } catch (IOException e) {
      throw new RuntimeException("Failed to prepare hive conf dir for ITs", e);
    }
  }

  private static void deleteHiveConfDir() {
    if (hiveConfDirPath == null) {
      return;
    }
    try (Stream<Path> walk = Files.walk(hiveConfDirPath)) {
      walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    } catch (IOException e) {
      LOG.warn("Failed to delete temp hive conf dir: {}", hiveConfDirPath, e);
    } finally {
      hiveConfDirPath = null;
      hiveConfDir = null;
    }
  }
}
