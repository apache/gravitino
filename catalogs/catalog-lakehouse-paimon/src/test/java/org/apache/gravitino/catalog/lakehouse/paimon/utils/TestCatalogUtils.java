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

package org.apache.gravitino.catalog.lakehouse.paimon.utils;

import static org.apache.gravitino.catalog.lakehouse.paimon.utils.CatalogUtils.loadCatalogBackend;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.Locale;
import java.util.function.Consumer;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogBackend;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConfig;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.factories.FactoryException;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.jdbc.JdbcCatalog;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Tests for {@link org.apache.gravitino.catalog.lakehouse.paimon.utils.CatalogUtils}. */
@Tag("gravitino-docker-test")
public class TestCatalogUtils {

  @Test
  void testLoadCatalogBackend() throws Exception {
    // Test load FileSystemCatalog for filesystem metastore.
    assertCatalog(PaimonCatalogBackend.FILESYSTEM.name(), FileSystemCatalog.class);
    // Test load JdbcCatalog for jdbc metastore.
    assertCatalog(PaimonCatalogBackend.JDBC.name(), JdbcCatalog.class);
    // Test load HiveCatalog for hive metastore.
    assertCatalog(PaimonCatalogBackend.HIVE.name(), HiveCatalog.class);
    // Test load catalog exception for other metastore.
    assertThrowsExactly(FactoryException.class, () -> assertCatalog("other", catalog -> {}));
  }

  private void assertCatalog(String metastore, Class<?> expected) throws Exception {
    assertCatalog(
        metastore.toLowerCase(Locale.ROOT), catalog -> assertEquals(expected, catalog.getClass()));
  }

  private void assertCatalog(String metastore, Consumer<Catalog> consumer) throws Exception {
    try (Catalog catalog =
        loadCatalogBackend(
                new PaimonConfig(
                    ImmutableMap.of(
                        PaimonConfig.CATALOG_BACKEND.getKey(),
                        metastore,
                        PaimonConfig.CATALOG_WAREHOUSE.getKey(),
                        String.join(
                            File.separator,
                            System.getProperty("java.io.tmpdir"),
                            "paimon_catalog_warehouse"),
                        PaimonConfig.CATALOG_URI.getKey(),
                        generateUri(metastore),
                        "cache-enabled",
                        "false",
                        PaimonConfig.CATALOG_JDBC_USER.getKey(),
                        "user",
                        PaimonConfig.CATALOG_JDBC_PASSWORD.getKey(),
                        "password",
                        PaimonConfig.CATALOG_JDBC_DRIVER.getKey(),
                        "org.h2.Driver")))
            .getCatalog()) {
      consumer.accept(catalog);
    }
  }

  private static String generateUri(String metastore) {
    String uri = "uri";
    if (PaimonCatalogBackend.JDBC.name().equalsIgnoreCase(metastore)) {
      uri = "jdbc:h2:mem:testdb";
    } else if (PaimonCatalogBackend.HIVE.name().equalsIgnoreCase(metastore)) {
      ContainerSuite containerSuite = ContainerSuite.getInstance();
      containerSuite.startHiveContainer();
      uri =
          String.format(
              "thrift://%s:%d",
              containerSuite.getHiveContainer().getContainerIpAddress(),
              HiveContainer.HIVE_METASTORE_PORT);
    }
    return uri;
  }
}
