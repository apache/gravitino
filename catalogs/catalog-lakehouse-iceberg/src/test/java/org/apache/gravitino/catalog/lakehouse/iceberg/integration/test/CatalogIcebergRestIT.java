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

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class CatalogIcebergRestIT extends CatalogIcebergBaseIT {

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

  /**
   * Regression guard for issue #11943. On the REST backend, {@code warehouse} is a catalog selector
   * (forwarded to the remote Iceberg REST server as {@code ?warehouse=}), not a storage location. A
   * value the REST server cannot resolve to a catalog must fail when the catalog is created —
   * validated as the property is taken in — instead of succeeding and then producing a confusing
   * {@code NoSuchWarehouseException} on first use.
   *
   * <p>The happy path (REST backend with {@code warehouse} omitted, resolving to the default
   * catalog) is exercised by the inherited {@link CatalogIcebergBaseIT} suite, which creates its
   * catalog without a {@code warehouse} property on the REST backend.
   */
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
}
