/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.iceberg.service.provider;

import java.util.HashMap;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestDynamicIcebergConfigProvider {
  @Test
  public void testValidIcebergTableOps() {
    String hiveCatalogName = "hive_backend";
    String jdbcCatalogName = "jdbc_backend";

    Catalog hiveMockCatalog = Mockito.mock(Catalog.class);
    Catalog jdbcMockCatalog = Mockito.mock(Catalog.class);

    GravitinoMetalake gravitinoMetalake = Mockito.mock(GravitinoMetalake.class);
    Mockito.when(gravitinoMetalake.loadCatalog(hiveCatalogName)).thenReturn(hiveMockCatalog);
    Mockito.when(gravitinoMetalake.loadCatalog(jdbcCatalogName)).thenReturn(jdbcMockCatalog);

    Mockito.when(hiveMockCatalog.provider()).thenReturn("lakehouse-iceberg");
    Mockito.when(jdbcMockCatalog.provider()).thenReturn("lakehouse-iceberg");

    Mockito.when(hiveMockCatalog.properties())
        .thenReturn(
            new HashMap<String, String>() {
              {
                put(IcebergConstants.CATALOG_BACKEND, "hive");
                put(IcebergConstants.URI, "thrift://127.0.0.1:7004");
                put(IcebergConstants.WAREHOUSE, "/tmp/usr/hive/warehouse");
                put(IcebergConstants.CATALOG_BACKEND_NAME, hiveCatalogName);
              }
            });
    Mockito.when(jdbcMockCatalog.properties())
        .thenReturn(
            new HashMap<String, String>() {
              {
                put(IcebergConstants.CATALOG_BACKEND, "jdbc");
                put(IcebergConstants.URI, "jdbc:sqlite::memory:");
                put(IcebergConstants.WAREHOUSE, "/tmp/user/hive/warehouse-jdbc");
                put(IcebergConstants.GRAVITINO_JDBC_USER, "gravitino");
                put(IcebergConstants.GRAVITINO_JDBC_PASSWORD, "gravitino");
                put(IcebergConstants.GRAVITINO_JDBC_DRIVER, "org.sqlite.JDBC");
                put(IcebergConstants.ICEBERG_JDBC_INITIALIZE, "true");
                put(IcebergConstants.CATALOG_BACKEND_NAME, jdbcCatalogName);
              }
            });

    DynamicIcebergConfigProvider provider = new DynamicIcebergConfigProvider();
    GravitinoAdminClient client = Mockito.mock(GravitinoAdminClient.class);
    Mockito.when(client.loadMetalake(Mockito.any())).thenReturn(gravitinoMetalake);
    provider.setClient(client);

    IcebergCatalogWrapper hiveOps =
        new IcebergCatalogWrapper(provider.getIcebergCatalogConfig(hiveCatalogName).get());
    IcebergCatalogWrapper jdbcOps =
        new IcebergCatalogWrapper(provider.getIcebergCatalogConfig(jdbcCatalogName).get());

    Assertions.assertEquals(hiveCatalogName, hiveOps.getCatalog().name());
    Assertions.assertEquals(jdbcCatalogName, jdbcOps.getCatalog().name());

    Assertions.assertTrue(hiveOps.getCatalog() instanceof HiveCatalog);
    Assertions.assertTrue(jdbcOps.getCatalog() instanceof JdbcCatalog);
  }

  @Test
  public void testInvalidIcebergTableOps() {
    String invalidCatalogName = "invalid_catalog";

    Catalog invalidCatalog = Mockito.mock(Catalog.class);

    GravitinoMetalake gravitinoMetalake = Mockito.mock(GravitinoMetalake.class);
    Mockito.when(gravitinoMetalake.loadCatalog(invalidCatalogName)).thenReturn(invalidCatalog);

    Mockito.when(invalidCatalog.provider()).thenReturn("hive");

    GravitinoAdminClient client = Mockito.mock(GravitinoAdminClient.class);
    Mockito.when(client.loadMetalake(Mockito.any())).thenReturn(gravitinoMetalake);

    DynamicIcebergConfigProvider provider = new DynamicIcebergConfigProvider();
    provider.setClient(client);

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class, () -> provider.getIcebergCatalogConfig(invalidCatalogName));
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class, () -> provider.getIcebergCatalogConfig(""));
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> provider.getIcebergCatalogConfig(IcebergConstants.ICEBERG_REST_DEFAULT_CATALOG));
  }
}
