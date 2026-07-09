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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class CatalogIcebergRestIT extends CatalogIcebergBaseIT {

  // A named (non-default) catalog registered on the embedded Iceberg REST service so a `warehouse`
  // selector that resolves to a real catalog can be exercised at create time.
  private static final String NAMED_CATALOG = "it_named_catalog";

  @Override
  @BeforeAll
  public void startup() throws Exception {
    // Register the named catalog before the server (and its Iceberg REST service) starts, so the
    // REST service serves `NAMED_CATALOG` in addition to its default catalog. Must run before
    // super.startup() launches the embedded server.
    String namedWarehouse = System.getProperty("java.io.tmpdir") + "/" + NAMED_CATALOG + "-wh";
    String prefix =
        "gravitino." + IcebergConstants.GRAVITINO_ICEBERG_REST_SERVICE_NAME + ".catalog.";
    registerCustomConfigs(
        ImmutableMap.of(
            prefix + NAMED_CATALOG + "." + IcebergConstants.CATALOG_BACKEND,
            "memory",
            prefix + NAMED_CATALOG + "." + IcebergConstants.WAREHOUSE,
            namedWarehouse));
    super.startup();
  }

  @Override
  protected void initIcebergCatalogProperties() {
    Map<String, String> map =
        serverConfig.getConfigsWithPrefix(
            String.format("gravitino.%s.", IcebergConstants.GRAVITINO_ICEBERG_REST_SERVICE_NAME));
    IcebergConfig icebergConfig = new IcebergConfig(map);
    String host = icebergConfig.get(JettyServerConfig.WEBSERVER_HOST);
    int port = icebergConfig.get(JettyServerConfig.WEBSERVER_HTTP_PORT);
    URIS = String.format("http://%s:%d/iceberg/", host, port);
    TYPE = "rest";
    WAREHOUSE =
        String.format(
            "hdfs://%s:%d/user/hive/warehouse-catalog-iceberg/",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT);
  }

  // Checks an Iceberg REST warehouse misconfiguration is caught on create. See issue #11943.
  @Test
  void testCreateRestCatalogWithUnresolvableWarehouseFailsAtCreate() {
    String badCatalogName = GravitinoITUtils.genRandomName("iceberg_rest_bad_warehouse");
    Map<String, String> properties = Maps.newHashMap();
    properties.put(IcebergConfig.CATALOG_BACKEND.getKey(), "rest");
    properties.put(IcebergConfig.CATALOG_URI.getKey(), URIS);
    // A URI-shaped value copied from a hive/jdbc example; the REST server does not know a catalog
    // by this name, so it cannot be resolved.
    properties.put(IcebergConfig.CATALOG_WAREHOUSE.getKey(), "s3://not-a-real-catalog/");

    // A reachable REST server rejecting the warehouse selector is a user/configuration error, so
    // it surfaces as an IllegalArgumentException (HTTP 400) at create time.
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                metalake.createCatalog(
                    badCatalogName,
                    Catalog.Type.RELATIONAL,
                    "lakehouse-iceberg",
                    "comment",
                    properties));

    Assertions.assertTrue(
        exception.getMessage() != null
            && exception.getMessage().contains("selects a catalog by name")
            && exception.getMessage().contains("s3://not-a-real-catalog/"),
        "expected the REST warehouse name-vs-location hint, but got: " + exception.getMessage());

    Assertions.assertFalse(
        metalake.catalogExists(badCatalogName),
        "catalog must not be created when its warehouse cannot be resolved at create time");
  }

  // A warehouse the REST server serves must still create and be usable. See issue #11943.
  @Test
  void testCreateRestCatalogWithValidWarehouseSucceeds() {
    String catalogName = GravitinoITUtils.genRandomName("iceberg_rest_named_warehouse");
    Map<String, String> properties = Maps.newHashMap();
    properties.put(IcebergConfig.CATALOG_BACKEND.getKey(), "rest");
    properties.put(IcebergConfig.CATALOG_URI.getKey(), URIS);
    // A valid selector: NAMED_CATALOG is a catalog the embedded REST server serves.
    properties.put(IcebergConfig.CATALOG_WAREHOUSE.getKey(), NAMED_CATALOG);

    Catalog created =
        metalake.createCatalog(
            catalogName, Catalog.Type.RELATIONAL, "lakehouse-iceberg", "comment", properties);
    try {
      Assertions.assertTrue(metalake.catalogExists(catalogName));
      // Resolvable end-to-end: listing schemas against the selected catalog succeeds.
      Assertions.assertDoesNotThrow(() -> created.asSchemas().listSchemas());
    } finally {
      metalake.disableCatalog(catalogName);
      metalake.dropCatalog(catalogName);
    }
  }
}
