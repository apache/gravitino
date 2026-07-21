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

import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata.GRAVITINO_JDBC_PASSWORD;
import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata.GRAVITINO_JDBC_USER;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.integration.test.container.MySQLContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Docker-backed integration tests for the {@code ClassLoaderPool} using a real MySQL Iceberg JDBC
 * backend. These complement the counting-only unit tests in {@code TestClassLoaderPoolIntegration}
 * by exercising the two runtime properties the pool actually claims:
 *
 * <ul>
 *   <li><b>Key isolation (the {@code uri} blind spot):</b> Iceberg's JDBC backend uses the {@code
 *       uri} catalog property, not {@code jdbc-url}. Two MySQL-backed Iceberg catalogs on different
 *       databases must NOT share a ClassLoader (otherwise they would cross-contaminate the
 *       per-ClassLoader {@code DriverManager} registry); two catalogs on the same {@code uri} must
 *       share one.
 *   <li><b>Lifecycle / no premature cleanup:</b> dropping one catalog that shares a ClassLoader
 *       must not deregister the JDBC driver (or shut down MySQL's {@code
 *       AbandonedConnectionCleanupThread}) while a sibling catalog is still live, and {@code
 *       testConnection} must acquire+release the shared key without breaking a live catalog.
 * </ul>
 *
 * <p>ClassLoader-identity assertions require reflecting into the in-process server, so they run
 * only in embedded mode; the end-to-end "still works after drop / testConnection" assertions run in
 * any mode via the client.
 */
@Tag("gravitino-docker-test")
public class IcebergClassLoaderPoolIT extends BaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergClassLoaderPoolIT.class);

  private static final TestDatabaseName DB_A =
      TestDatabaseName.MYSQL_TEST_ICEBERG_CLASSLOADER_POOL_A;
  private static final TestDatabaseName DB_B =
      TestDatabaseName.MYSQL_TEST_ICEBERG_CLASSLOADER_POOL_B;
  private static final String PROVIDER = "lakehouse-iceberg";

  private static MySQLContainer mySQLContainer;

  // Metalakes created by tests, dropped (force) in tearDown so no state leaks across tests/runs
  // (important in deploy mode where the entity store persists between runs).
  private final List<String> createdMetalakes = new ArrayList<>();

  @BeforeAll
  public void startup() throws IOException {
    containerSuite.startMySQLContainer(DB_A);
    mySQLContainer = containerSuite.getMySQLContainer();
    // Second database on the same MySQL container to produce a distinct backend URI.
    mySQLContainer.createDatabase(DB_B);
  }

  @AfterEach
  public void cleanup() {
    for (String metalakeName : createdMetalakes) {
      try {
        client.dropMetalake(metalakeName, true);
      } catch (Exception e) {
        LOG.warn("Failed to drop metalake {} during cleanup", metalakeName, e);
      }
    }
    createdMetalakes.clear();
  }

  private GravitinoMetalake createMetalake(String name) {
    GravitinoMetalake metalake = client.createMetalake(name, "comment", Collections.emptyMap());
    createdMetalakes.add(name);
    return metalake;
  }

  private Map<String, String> icebergMysqlConf(TestDatabaseName db) throws Exception {
    Map<String, String> conf = Maps.newHashMap();
    // Scope JDBC metadata lookups to the connected database via nullCatalogMeansCurrent=true.
    // These tests deliberately place DB_A and DB_B on the SAME MySQL server (to produce two
    // distinct backend URIs for the ClassLoader isolation assertion). Iceberg's JdbcCatalog
    // checks whether its `iceberg_tables` control table already exists with
    // getTables(catalog=null, schema=null, "iceberg_tables", null) before creating it. Under
    // Connector/J's default (nullCatalogMeansCurrent=false) that call scans EVERY database on the
    // server, so a catalog on DB_B would "see" DB_A's iceberg_tables, skip creating its own, and
    // then fail real queries with "table doesn't exist". Setting nullCatalogMeansCurrent=true
    // scopes the check to the current database. (This mirrors the recommended setting for running
    // multiple Iceberg JDBC catalogs against separate databases of one MySQL server.)
    conf.put(
        IcebergConfig.CATALOG_URI.getKey(),
        mySQLContainer.getJdbcUrl(db) + "?nullCatalogMeansCurrent=true");
    conf.put(IcebergConfig.CATALOG_BACKEND.getKey(), "jdbc");
    // Unique backend name per catalog so each catalog's JDBC namespace registry
    // (Iceberg's iceberg_namespace_properties, keyed by the initialize() name) is isolated even
    // when catalogs share a database. Without this, all catalogs default to backend name "jdbc"
    // and would see each other's namespaces, causing cross-test contamination.
    // `catalog-backend-name`
    // is NOT in DEFAULT_ISOLATION_PROPERTY_KEYS, so catalogs on the same `uri` still share a
    // ClassLoader (which is what the sharing/lifecycle assertions rely on).
    conf.put(
        IcebergConfig.CATALOG_BACKEND_NAME.getKey(), RandomNameUtils.genRandomName("clp_backend"));
    // Unique warehouse dir per catalog to avoid stale /tmp state leaking across catalogs and runs.
    conf.put(
        IcebergConfig.CATALOG_WAREHOUSE.getKey(),
        "file:///tmp/" + RandomNameUtils.genRandomName("iceberg_clp_wh"));
    conf.put(IcebergConfig.JDBC_DRIVER.getKey(), mySQLContainer.getDriverClassName(db));
    conf.put(GRAVITINO_JDBC_USER, mySQLContainer.getUsername());
    conf.put(GRAVITINO_JDBC_PASSWORD, mySQLContainer.getPassword());
    return conf;
  }

  private boolean isEmbedded() {
    return ITUtils.EMBEDDED_TEST_MODE.equals(testMode);
  }

  private Object classLoaderOf(String metalake, String catalog) throws IllegalAccessException {
    CatalogManager catalogManager = GravitinoEnv.getInstance().catalogManager();
    // Use loadCatalogAndWrap (rather than the Caffeine cache directly) so this test module does not
    // need caffeine on its compile classpath. The catalog was already loaded server-side above, so
    // this returns the cached wrapper instance.
    CatalogManager.CatalogWrapper wrapper =
        catalogManager.loadCatalogAndWrap(NameIdentifier.of(metalake, catalog));
    Assertions.assertNotNull(wrapper, "wrapper should be cached for " + metalake + "." + catalog);
    return FieldUtils.readField(wrapper, "classLoader", true);
  }

  @Test
  public void testSameUriShareAndDifferentUriIsolateClassLoader() throws Exception {
    Assumptions.assumeTrue(
        isEmbedded(),
        "ClassLoader-identity assertions require the in-process server (embedded mode)");

    String metalakeName = RandomNameUtils.genRandomName("clp_metalake");
    GravitinoMetalake metalake = createMetalake(metalakeName);

    // Two catalogs on the SAME uri (DB_A) must share one ClassLoader.
    String sameA1 = RandomNameUtils.genRandomName("clp_same_a1");
    String sameA2 = RandomNameUtils.genRandomName("clp_same_a2");
    metalake.createCatalog(
        sameA1, Catalog.Type.RELATIONAL, PROVIDER, "comment", icebergMysqlConf(DB_A));
    metalake.createCatalog(
        sameA2, Catalog.Type.RELATIONAL, PROVIDER, "comment", icebergMysqlConf(DB_A));

    // Force server-side load so both wrappers are cached.
    metalake.loadCatalog(sameA1);
    metalake.loadCatalog(sameA2);

    Assertions.assertSame(
        classLoaderOf(metalakeName, sameA1),
        classLoaderOf(metalakeName, sameA2),
        "Iceberg catalogs sharing the same backend uri must share a ClassLoader");

    // A catalog on a DIFFERENT uri (DB_B) must NOT share the ClassLoader — this is the `uri`
    // isolation-key fix; without `uri` in DEFAULT_ISOLATION_PROPERTY_KEYS these would collide.
    String otherB = RandomNameUtils.genRandomName("clp_other_b");
    metalake.createCatalog(
        otherB, Catalog.Type.RELATIONAL, PROVIDER, "comment", icebergMysqlConf(DB_B));
    metalake.loadCatalog(otherB);

    Assertions.assertNotSame(
        classLoaderOf(metalakeName, sameA1),
        classLoaderOf(metalakeName, otherB),
        "Iceberg catalogs with different backend uris must NOT share a ClassLoader");
  }

  @Test
  public void testDropOneSharedCatalogKeepsSiblingUsable() throws Exception {
    String metalakeName = RandomNameUtils.genRandomName("clp_metalake");
    GravitinoMetalake metalake = createMetalake(metalakeName);

    String catalog1 = RandomNameUtils.genRandomName("clp_drop_1");
    String catalog2 = RandomNameUtils.genRandomName("clp_drop_2");
    metalake.createCatalog(
        catalog1, Catalog.Type.RELATIONAL, PROVIDER, "comment", icebergMysqlConf(DB_A));
    Catalog cat2 =
        metalake.createCatalog(
            catalog2, Catalog.Type.RELATIONAL, PROVIDER, "comment", icebergMysqlConf(DB_A));

    // Exercise catalog2's real JDBC backend once so its driver is registered/used.
    Assertions.assertEquals(0, cat2.asSchemas().listSchemas().length);

    // Drop catalog1 — since it shares the pooled ClassLoader (and the MySQL driver /
    // AbandonedConnectionCleanupThread) with catalog2, the final-release cleanup must NOT run.
    metalake.disableCatalog(catalog1);
    Assertions.assertTrue(metalake.dropCatalog(catalog1, true));

    // Issue a REAL query against catalog2 after catalog1's drop. If the shared driver had been
    // deregistered / the cleanup thread shut down, this JDBC call would fail.
    Catalog reloaded = metalake.loadCatalog(catalog2);
    String schemaName = RandomNameUtils.genRandomName("clp_schema");
    Assertions.assertDoesNotThrow(
        () -> reloaded.asSchemas().createSchema(schemaName, null, Collections.emptyMap()));
    Assertions.assertTrue(reloaded.asSchemas().schemaExists(schemaName));
  }

  @Test
  public void testTestConnectionKeepsLiveSharedCatalogUsable() throws Exception {
    String metalakeName = RandomNameUtils.genRandomName("clp_metalake");
    GravitinoMetalake metalake = createMetalake(metalakeName);

    String liveCatalog = RandomNameUtils.genRandomName("clp_live");
    Catalog live =
        metalake.createCatalog(
            liveCatalog, Catalog.Type.RELATIONAL, PROVIDER, "comment", icebergMysqlConf(DB_A));
    Assertions.assertEquals(0, live.asSchemas().listSchemas().length);

    // testConnection builds a throwaway wrapper that acquires + releases the SAME pooled key.
    // The release must only decrement the refCount, not run the destructive final cleanup, because
    // the live catalog still shares the ClassLoader / driver.
    String probeCatalog = RandomNameUtils.genRandomName("clp_probe");
    Assertions.assertDoesNotThrow(
        () ->
            metalake.testConnection(
                probeCatalog,
                Catalog.Type.RELATIONAL,
                PROVIDER,
                "comment",
                icebergMysqlConf(DB_A)));

    // The live catalog must still perform real JDBC work after the probe's acquire/release cycle.
    Catalog reloaded = metalake.loadCatalog(liveCatalog);
    String schemaName = RandomNameUtils.genRandomName("clp_schema");
    Assertions.assertDoesNotThrow(
        () -> reloaded.asSchemas().createSchema(schemaName, null, Collections.emptyMap()));
    Assertions.assertTrue(reloaded.asSchemas().schemaExists(schemaName));
  }
}
