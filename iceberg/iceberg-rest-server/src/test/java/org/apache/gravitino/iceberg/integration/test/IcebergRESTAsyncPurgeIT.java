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
package org.apache.gravitino.iceberg.integration.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end test for async table purge. A table dropped with {@code purgeRequested=true} plus the
 * {@link IcebergRequestContext#ASYNC_PURGE_HEADER} should keep its files on disk while the drop
 * returns, block recreating the same name with {@code 409} until cleanup finishes, and have its
 * files deleted by the background worker.
 *
 * <p>It drives the server with the {@link RESTCatalog} client, not Spark: Spark purges files
 * client-side and sends {@code purgeRequested=false}, which skips the server-side path. It uses the
 * dynamic config provider over a PostgreSQL-backed {@code lakehouse-iceberg} catalog with a local
 * {@code file://} warehouse, since cleanup jobs are keyed by catalog id and the files must be
 * visible on disk.
 */
public class IcebergRESTAsyncPurgeIT extends BaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergRESTAsyncPurgeIT.class);
  private static final String GRAVITINO_ICEBERG_REST_PREFIX = "gravitino.iceberg-rest.";
  private static final String METALAKE_NAME = "async_cleanup_metalake";
  private static final String CATALOG_NAME = "iceberg_cleanup";
  private static final String USER = "test";
  private static final String DATABASE_NAME = "purge_db";
  private static final String TABLE_NAME = "t_async";
  // Poll interval large enough that the background worker provably cannot claim the job during the
  // synchronous in-flight assertions, yet small enough to keep the eventual-cleanup wait short.
  private static final int CLEANUP_POLL_INTERVAL_SECS = 5;

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private static final ContainerSuite CONTAINER_SUITE = ContainerSuite.getInstance();

  private Path warehouseDir;
  private RESTCatalog restCatalog;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    CONTAINER_SUITE.startPostgreSQLContainer(TestDatabaseName.PG_ICEBERG_ASYNC_CLEANUP_IT);
    warehouseDir = Files.createTempDirectory("gravitino-iceberg-async-cleanup");
    ignoreIcebergAuxRestService = false;

    // Simple authentication so the dynamic config provider can authenticate to Gravitino and the
    // REST client can identify itself; authorization stays disabled to keep the test focused.
    customConfigs.put("gravitino.authenticators", "simple");
    customConfigs.put("SimpleAuthUserName", USER);
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.ICEBERG_REST_CATALOG_CONFIG_PROVIDER,
        IcebergConstants.DYNAMIC_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME);
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_METALAKE, METALAKE_NAME);
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.ICEBERG_REST_DEFAULT_DYNAMIC_CATALOG_NAME,
        CATALOG_NAME);
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_SIMPLE_USERNAME, USER);
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConfig.ASYNC_CLEANUP_POLL_INTERVAL_SECS.getKey(),
        String.valueOf(CLEANUP_POLL_INTERVAL_SECS));

    super.startIntegrationTest();
    initMetalakeAndCatalog();
    initRESTCatalog();
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    if (restCatalog != null) {
      try {
        restCatalog.close();
      } catch (IOException e) {
        LOG.warn("Failed to close Iceberg REST catalog", e);
      }
      restCatalog = null;
    }
    try {
      client.dropMetalake(METALAKE_NAME, true);
    } catch (Exception e) {
      LOG.warn("Failed to drop metalake {}", METALAKE_NAME, e);
    }
    super.stopIntegrationTest();
    if (warehouseDir != null) {
      FileUtils.deleteQuietly(warehouseDir.toFile());
    }
  }

  @Test
  void testAsyncPurgeEndToEnd() {
    restCatalog.createNamespace(Namespace.of(DATABASE_NAME));
    TableIdentifier identifier = TableIdentifier.of(DATABASE_NAME, TABLE_NAME);
    Table table = restCatalog.createTable(identifier, SCHEMA);
    appendDataFile(table);

    Path tableDir = warehouseDir.resolve(DATABASE_NAME).resolve(TABLE_NAME);
    Assertions.assertTrue(
        countRegularFiles(tableDir) > 0,
        "Table should have metadata and data files on disk before drop");

    // Drop with purgeRequested=true; the async purge header routes this through the cleanup
    // manager.
    restCatalog.dropTable(identifier, true);

    // The worker polls every few seconds, so right after the drop returns the files must still be
    // on disk. Synchronous purge would have already deleted them, so this is what proves the drop
    // went through the async path.
    Assertions.assertTrue(
        countRegularFiles(tableDir) > 0,
        "Async purge must not delete files synchronously during the drop request");

    // While the cleanup job is in flight, the dropped name is occupied and cannot be recreated.
    AlreadyExistsException occupied =
        Assertions.assertThrows(
            AlreadyExistsException.class, () -> restCatalog.createTable(identifier, SCHEMA));
    Assertions.assertTrue(
        occupied.getMessage().contains("being purged"),
        "Expected a 'being purged' conflict, got: " + occupied.getMessage());

    // The background cleanup worker eventually deletes every file and releases the name.
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () ->
                Assertions.assertEquals(
                    0L,
                    countRegularFiles(tableDir),
                    "Background cleanup worker should have deleted all table files"));
  }

  private void initMetalakeAndCatalog() {
    GravitinoMetalake metalake = client.createMetalake(METALAKE_NAME, "", new HashMap<>());
    Map<String, String> props = Maps.newHashMap();
    props.put(IcebergConstants.CATALOG_BACKEND, "jdbc");
    props.put(
        IcebergConstants.URI,
        CONTAINER_SUITE
            .getPostgreSQLContainer()
            .getJdbcUrl(TestDatabaseName.PG_ICEBERG_ASYNC_CLEANUP_IT));
    props.put(IcebergConstants.GRAVITINO_JDBC_DRIVER, "org.postgresql.Driver");
    props.put(
        IcebergConstants.GRAVITINO_JDBC_USER,
        CONTAINER_SUITE.getPostgreSQLContainer().getUsername());
    props.put(
        IcebergConstants.GRAVITINO_JDBC_PASSWORD,
        CONTAINER_SUITE.getPostgreSQLContainer().getPassword());
    props.put("gravitino.bypass.jdbc.schema-version", "v1");
    props.put(IcebergConstants.ICEBERG_JDBC_INITIALIZE, "true");
    props.put(IcebergConstants.WAREHOUSE, warehouseDir.toUri().toString());

    metalake.createCatalog(
        CATALOG_NAME, Catalog.Type.RELATIONAL, "lakehouse-iceberg", "async cleanup IT", props);
  }

  private void appendDataFile(Table table) {
    // Purge deletes files by reachability, not content, so a placeholder file is enough to exercise
    // data-file cleanup alongside the manifests and metadata the commit writes.
    String dataPath = table.location() + "/data/async-purge-it-0.parquet";
    OutputFile outputFile = table.io().newOutputFile(dataPath);
    try (PositionOutputStream out = outputFile.create()) {
      out.write(new byte[] {1, 2, 3, 4});
    } catch (IOException e) {
      throw new RuntimeException("Failed to write placeholder data file " + dataPath, e);
    }
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(dataPath)
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(4L)
            .withRecordCount(1L)
            .build();
    table.newAppend().appendFile(dataFile).commit();
  }

  private void initRESTCatalog() {
    String icebergRESTUri = getIcebergRestServiceUri();
    LOG.info("Iceberg REST uri: {}", icebergRESTUri);
    Map<String, String> props = new HashMap<>();
    props.put(CatalogProperties.URI, icebergRESTUri);
    props.put(CatalogProperties.CACHE_ENABLED, "false");
    props.put("rest.auth.type", "basic");
    props.put("rest.auth.basic.username", USER);
    props.put("rest.auth.basic.password", "mock");
    // Opt every request from this client into async purge.
    props.put("header." + IcebergRequestContext.ASYNC_PURGE_HEADER, "true");
    RESTCatalog catalog = new RESTCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize("async_purge", ImmutableMap.copyOf(props));
    restCatalog = catalog;
  }

  private long countRegularFiles(Path dir) {
    if (!Files.exists(dir)) {
      return 0L;
    }
    try (Stream<Path> paths = Files.walk(dir)) {
      return paths.filter(Files::isRegularFile).count();
    } catch (IOException e) {
      throw new RuntimeException("Failed to list files under " + dir, e);
    }
  }
}
