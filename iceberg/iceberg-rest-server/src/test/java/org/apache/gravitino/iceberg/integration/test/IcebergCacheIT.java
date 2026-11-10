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

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.cache.LocalTableMetadataCache;
import org.apache.gravitino.iceberg.common.cache.SupportsMetadataLocation;
import org.apache.gravitino.iceberg.common.cache.TableMetadataCache;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.integration.test.util.IcebergRESTServerManager;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.memory.MemoryCatalogWithMetadataLocationSupport;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

/** Docker IT for default table metadata cache in {@link IcebergCatalogWrapper}. */
@Tag("gravitino-docker-test")
@TestInstance(Lifecycle.PER_CLASS)
public class IcebergCacheIT {

  private static final ContainerSuite CONTAINER_SUITE = ContainerSuite.getInstance();

  private static final Schema TABLE_SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  private IcebergRESTServerManager restServer;

  @BeforeAll
  void startDockerEnv() throws Exception {
    CONTAINER_SUITE.startHiveContainer();
    restServer = IcebergRESTServerManager.create();
    Map<String, String> serverConfigs = new HashMap<>();
    serverConfigs.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.CATALOG_BACKEND.getKey(),
        IcebergCatalogBackend.HIVE.toString().toLowerCase());
    serverConfigs.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.CATALOG_URI.getKey(),
        hiveMetastoreUri());
    serverConfigs.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.CATALOG_WAREHOUSE.getKey(),
        hdfsWarehouse("iceberg-rest-cache-it"));
    restServer.registerCustomConfigs(serverConfigs);
    restServer.startIcebergRESTServer();
  }

  @AfterAll
  void stopDockerEnv() throws Exception {
    if (restServer != null) {
      restServer.stopIcebergRESTServer();
    }
  }

  @Test
  void testRestCacheDisabled() throws Exception {
    int port = restPort();
    Map<String, String> properties = new HashMap<>();
    properties.put(
        IcebergConstants.CATALOG_BACKEND, IcebergCatalogBackend.REST.name().toLowerCase());
    properties.put(IcebergConstants.URI, String.format("http://127.0.0.1:%d/iceberg/", port));
    properties.put(IcebergConstants.CATALOG_BACKEND_NAME, "rest_cache_it");

    IcebergConfig icebergConfig = new IcebergConfig(properties);
    Assertions.assertEquals(
        LocalTableMetadataCache.class.getName(),
        icebergConfig.get(IcebergConfig.TABLE_METADATA_CACHE_IMPL));

    try (IcebergCatalogWrapper wrapper = new IcebergCatalogWrapper(icebergConfig)) {
      TableMetadataCache cache = metadataCache(wrapper);
      Assertions.assertSame(TableMetadataCache.DUMMY, cache);
    }
  }

  @Test
  void testCustomNoSupportLocationOff() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(
        IcebergConstants.CATALOG_BACKEND, IcebergCatalogBackend.CUSTOM.name().toLowerCase());
    properties.put(
        IcebergConstants.CATALOG_BACKEND_IMPL, CustomNoSupportLocationCatalog.class.getName());
    properties.put(IcebergConstants.CATALOG_BACKEND_NAME, "custom_no_support_loc");
    properties.put(IcebergConstants.URI, hiveMetastoreUri());
    properties.put(IcebergConstants.WAREHOUSE, hdfsWarehouse("custom-no-support-loc"));

    IcebergConfig icebergConfig = new IcebergConfig(properties);
    Assertions.assertEquals(
        LocalTableMetadataCache.class.getName(),
        icebergConfig.get(IcebergConfig.TABLE_METADATA_CACHE_IMPL));

    try (IcebergCatalogWrapper wrapper = new IcebergCatalogWrapper(icebergConfig)) {
      TableMetadataCache cache = metadataCache(wrapper);
      Assertions.assertSame(TableMetadataCache.DUMMY, cache);
    }
  }

  @Test
  void testCustomSupportLocationOn() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(
        IcebergConstants.CATALOG_BACKEND, IcebergCatalogBackend.CUSTOM.name().toLowerCase());
    properties.put(
        IcebergConstants.CATALOG_BACKEND_IMPL, CustomSupportLocationCatalog.class.getName());
    properties.put(IcebergConstants.CATALOG_BACKEND_NAME, "custom_support_loc");
    properties.put(IcebergConstants.URI, hiveMetastoreUri());
    properties.put(IcebergConstants.WAREHOUSE, hdfsWarehouse("custom-support-loc"));

    IcebergConfig icebergConfig = new IcebergConfig(properties);
    Assertions.assertEquals(
        LocalTableMetadataCache.class.getName(),
        icebergConfig.get(IcebergConfig.TABLE_METADATA_CACHE_IMPL));
    Assertions.assertEquals(1000, icebergConfig.get(IcebergConfig.TABLE_METADATA_CACHE_CAPACITY));

    Namespace namespace = Namespace.of("cache_ns");
    TableIdentifier tableId = TableIdentifier.of(namespace, "cache_tbl");

    try (IcebergCatalogWrapper wrapper = new IcebergCatalogWrapper(icebergConfig)) {
      TableMetadataCache cache = metadataCache(wrapper);
      Assertions.assertInstanceOf(LocalTableMetadataCache.class, cache);

      wrapper.createNamespace(CreateNamespaceRequest.builder().withNamespace(namespace).build());
      LoadTableResponse createResponse =
          wrapper.createTable(
              namespace,
              CreateTableRequest.builder()
                  .withName(tableId.name())
                  .withSchema(TABLE_SCHEMA)
                  .build());
      Assertions.assertNotNull(createResponse);

      Assertions.assertTrue(cache.getTableMetadata(tableId).isPresent());

      LoadTableResponse loadResponse = wrapper.loadTable(tableId);
      Assertions.assertNotNull(loadResponse);
      Assertions.assertTrue(cache.getTableMetadata(tableId).isPresent());
    }
  }

  private static String hiveMetastoreUri() {
    return String.format(
        "thrift://%s:%d",
        CONTAINER_SUITE.getHiveContainer().getContainerIpAddress(),
        HiveContainer.HIVE_METASTORE_PORT);
  }

  private static String hdfsWarehouse(String suffix) {
    return GravitinoITUtils.genRandomName(
        String.format(
            "hdfs://%s:%d/user/hive/warehouse-%s",
            CONTAINER_SUITE.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT,
            suffix));
  }

  private int restPort() {
    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(
            restServer.getServerConfig(), IcebergConfig.ICEBERG_CONFIG_PREFIX);
    return jettyServerConfig.getHttpPort();
  }

  private static TableMetadataCache metadataCache(IcebergCatalogWrapper wrapper) throws Exception {
    Method method = IcebergCatalogWrapper.class.getDeclaredMethod("getMetadataCache");
    method.setAccessible(true);
    return (TableMetadataCache) method.invoke(wrapper);
  }

  /** Custom catalog that does not support metadata location for cache IT. */
  public static class CustomNoSupportLocationCatalog implements Catalog {

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return List.of();
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      return false;
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {}

    @Override
    public Table loadTable(TableIdentifier identifier) {
      return null;
    }
  }

  /** Custom catalog that supports metadata location for cache IT. */
  public static class CustomSupportLocationCatalog extends MemoryCatalogWithMetadataLocationSupport
      implements SupportsMetadataLocation {}
}
