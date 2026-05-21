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
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.cache.LocalTableMetadataCache;
import org.apache.gravitino.iceberg.common.cache.TableMetadataCache;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Verifies that IRC server-side {@code catalog.<name>.*} settings merge into dynamic catalog
 * configs. Uses an isolated in-memory JDBC catalog to avoid sharing state with authorization ITs.
 */
public class IcebergDynamicServerConfigMergeIT extends BaseIT {

  private static final String GRAVITINO_ICEBERG_REST_PREFIX = "gravitino.iceberg-rest.";
  private static final String METALAKE_NAME = "test_metalake_dynamic_server_config_merge";
  private static final String CATALOG_NAME = "iceberg";
  private static final String CATALOG_BACKEND_NAME = "dynamic_server_config_merge";
  private static final String SCHEMA_NAME = "dynamic_server_config_merge_schema";
  private static final String TABLE_NAME = "merge_config_table";
  private static final String JDBC_URI =
      "jdbc:sqlite::memory:gravitino_dynamic_server_config_merge";
  private static final String WAREHOUSE = "file:///tmp/gravitino_dynamic_server_config_merge/";
  private static final String SUPER_USER = "super";

  private GravitinoMetalake metalakeClient;
  private Catalog catalogClient;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    startGravitinoServerWithIcebergREST();
    initMetalakeAndCatalog();
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    if (client != null) {
      client.dropMetalake(METALAKE_NAME, true);
    }
    super.stopIntegrationTest();
  }

  @Test
  void testDynamicCatalogMergesCatalogPrefixedServerMetadataCacheConfig() {
    IcebergConfig icebergConfig =
        IcebergRESTServerContext.getInstance()
            .catalogWrapperManager()
            .getCatalogWrapper(IcebergConstants.ICEBERG_REST_DEFAULT_CATALOG)
            .getIcebergConfig();

    Assertions.assertEquals(
        LocalTableMetadataCache.class.getName(),
        icebergConfig.get(IcebergConfig.TABLE_METADATA_CACHE_IMPL));
  }

  @Test
  void testLoadTableUsesMergedCatalogPrefixedMetadataCache() throws Exception {
    if (!catalogClient.asSchemas().schemaExists(SCHEMA_NAME)) {
      catalogClient.asSchemas().createSchema(SCHEMA_NAME, "comment", new HashMap<>());
    }
    NameIdentifier tableIdent = NameIdentifier.of(SCHEMA_NAME, TABLE_NAME);
    if (!catalogClient.asTableCatalog().tableExists(tableIdent)) {
      catalogClient
          .asTableCatalog()
          .createTable(
              tableIdent,
              new Column[] {Column.of("id", Types.IntegerType.get(), "id")},
              "comment",
              new HashMap<>());
    }

    CatalogWrapperForREST catalogWrapper =
        IcebergRESTServerContext.getInstance()
            .catalogWrapperManager()
            .getCatalogWrapper(IcebergConstants.ICEBERG_REST_DEFAULT_CATALOG);
    catalogWrapper.loadTable(TableIdentifier.of(SCHEMA_NAME, TABLE_NAME));

    TableMetadataCache metadataCache = invokeGetMetadataCache(catalogWrapper);
    Assertions.assertInstanceOf(LocalTableMetadataCache.class, metadataCache);
    Assertions.assertNotSame(TableMetadataCache.DUMMY, metadataCache);
  }

  private void startGravitinoServerWithIcebergREST() throws Exception {
    ignoreIcebergAuxRestService = false;
    customConfigs.putAll(
        ImmutableMap.of(
            "gravitino.authorization.serviceAdmins",
            SUPER_USER,
            "gravitino.authenticators",
            "simple",
            "SimpleAuthUserName",
            SUPER_USER,
            Configs.ENABLE_AUTHORIZATION.getKey(),
            "true",
            Configs.AUTHORIZATION_IMPL.getKey(),
            JcasbinAuthorizer.class.getCanonicalName(),
            Configs.CACHE_ENABLED.getKey(),
            "true"));

    String namedCatalogPrefix = GRAVITINO_ICEBERG_REST_PREFIX + "catalog." + CATALOG_NAME + ".";
    Map<String, String> icebergRestConfigs = new HashMap<>();
    icebergRestConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.ICEBERG_REST_CATALOG_CONFIG_PROVIDER,
        IcebergConstants.DYNAMIC_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME);
    icebergRestConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_METALAKE, METALAKE_NAME);
    icebergRestConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.ICEBERG_REST_DEFAULT_DYNAMIC_CATALOG_NAME,
        CATALOG_NAME);
    icebergRestConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_SIMPLE_USERNAME, SUPER_USER);
    icebergRestConfigs.put(
        namedCatalogPrefix + IcebergConstants.TABLE_METADATA_CACHE_IMPL,
        LocalTableMetadataCache.class.getName());
    customConfigs.putAll(icebergRestConfigs);
    super.startIntegrationTest();
  }

  private void initMetalakeAndCatalog() {
    metalakeClient = client.createMetalake(METALAKE_NAME, "", new HashMap<>());

    Map<String, String> catalogProps = new HashMap<>();
    catalogProps.put(IcebergConstants.URI, JDBC_URI);
    catalogProps.put(IcebergConstants.CATALOG_BACKEND, "jdbc");
    catalogProps.put(IcebergConstants.CATALOG_BACKEND_NAME, CATALOG_BACKEND_NAME);
    catalogProps.put(IcebergConstants.GRAVITINO_JDBC_DRIVER, "org.sqlite.JDBC");
    catalogProps.put(IcebergConstants.ICEBERG_JDBC_INITIALIZE, "true");
    catalogProps.put("gravitino.bypass.jdbc.schema-version", "v1");
    catalogProps.put(IcebergConstants.WAREHOUSE, WAREHOUSE);

    catalogClient =
        metalakeClient.createCatalog(
            CATALOG_NAME, Catalog.Type.RELATIONAL, "lakehouse-iceberg", "comment", catalogProps);
  }

  private static TableMetadataCache invokeGetMetadataCache(IcebergCatalogWrapper wrapper)
      throws Exception {
    Method method = IcebergCatalogWrapper.class.getDeclaredMethod("getMetadataCache");
    method.setAccessible(true);
    return (TableMetadataCache) method.invoke(wrapper);
  }
}
