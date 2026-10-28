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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import com.google.common.collect.ImmutableMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergAuxCatalogBridge;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergGcsTokenRefresh {

  @AfterEach
  void tearDown() {
    IcebergAuxCatalogBridge.clear();
  }

  @Test
  void fixedBackendSkipsRefreshWithoutExpiry() {
    IcebergCatalogWrapper wrapper = new IcebergCatalogWrapper(memoryConfig());
    IcebergBackend backend = IcebergBackend.fixed(wrapper);
    Assertions.assertSame(wrapper, backend.wrapper());
    Assertions.assertSame(wrapper, backend.wrapper());
  }

  @Test
  void ownedBackendRecordsCatalogCacheDeadlineFromToken() {
    long expiresAt = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.WAREHOUSE,
                "/tmp/wh",
                IcebergConstants.ICEBERG_GCS_OAUTH2_TOKEN_EXPIRES_AT,
                String.valueOf(expiresAt)));
    IcebergBackend backend = IcebergBackend.createOwned(config.getAllConfig());
    Assertions.assertEquals(
        expiresAt - TimeUnit.MINUTES.toMillis(5), backend.cacheDeadlineEpochMs());
    backend.close();
  }

  @Test
  void auxModeKeepsTheFirstCatalogCacheDeadline() {
    long firstExpiresAt = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(2);
    long laterExpiresAt = firstExpiresAt + TimeUnit.HOURS.toMillis(3);
    IcebergCatalogWrapper first = new IcebergCatalogWrapper(configWithExpiry(firstExpiresAt));
    IcebergCatalogWrapper later = new IcebergCatalogWrapper(configWithExpiry(laterExpiresAt));
    AtomicReference<IcebergCatalogWrapper> current = new AtomicReference<>(first);
    IcebergAuxCatalogBridge.register(name -> current.get(), null);

    IcebergBackend backend =
        IcebergBackend.create(
            "orders", "metalake", ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "memory"));
    Assertions.assertSame(first, backend.wrapper());
    Assertions.assertEquals(
        firstExpiresAt - TimeUnit.MINUTES.toMillis(5), backend.cacheDeadlineEpochMs());
    current.set(later);
    Assertions.assertSame(later, backend.wrapper());
    Assertions.assertEquals(
        firstExpiresAt - TimeUnit.MINUTES.toMillis(5), backend.cacheDeadlineEpochMs());
    backend.close();
    Assertions.assertDoesNotThrow(first::close);
    Assertions.assertDoesNotThrow(later::close);
  }

  @Test
  void fixedBackendSkipsRefreshWhenTokenFarFromExpiry() {
    long expiresAt = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.WAREHOUSE,
                "/tmp/wh",
                IcebergConstants.ICEBERG_GCS_OAUTH2_TOKEN_EXPIRES_AT,
                String.valueOf(expiresAt)));
    IcebergCatalogWrapper wrapper = new IcebergCatalogWrapper(config);
    IcebergBackend backend = IcebergBackend.fixed(wrapper);
    Assertions.assertSame(wrapper, backend.wrapper());
  }

  private static IcebergConfig memoryConfig() {
    return new IcebergConfig(
        ImmutableMap.of(
            IcebergConstants.CATALOG_BACKEND, "memory", IcebergConstants.WAREHOUSE, "/tmp/wh"));
  }

  private static IcebergConfig configWithExpiry(long expiresAt) {
    return new IcebergConfig(
        ImmutableMap.of(
            IcebergConstants.CATALOG_BACKEND,
            "memory",
            IcebergConstants.WAREHOUSE,
            "/tmp/wh",
            IcebergConstants.ICEBERG_GCS_OAUTH2_TOKEN_EXPIRES_AT,
            String.valueOf(expiresAt)));
  }
}
