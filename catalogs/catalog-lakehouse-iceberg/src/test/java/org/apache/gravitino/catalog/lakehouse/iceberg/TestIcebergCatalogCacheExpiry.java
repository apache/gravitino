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
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergCatalogCacheExpiry {

  private static final long BUFFER_MS = TimeUnit.MINUTES.toMillis(5);

  @Test
  void refreshDeadlineEmptyWithoutGcsTokenExpiry() {
    IcebergCatalogOperations ops = new IcebergCatalogOperations();
    ops.icebergCatalogWrapper = new IcebergCatalogWrapper(memoryConfig());
    Assertions.assertFalse(ops.catalogCacheRefreshAtEpochMs().isPresent());
  }

  @Test
  void refreshDeadlineIsTokenExpiryMinusBuffer() {
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
    IcebergCatalogOperations ops = new IcebergCatalogOperations();
    ops.icebergCatalogWrapper = new IcebergCatalogWrapper(config);
    Assertions.assertEquals(expiresAt - BUFFER_MS, ops.catalogCacheRefreshAtEpochMs().orElse(-1L));
  }

  private static IcebergConfig memoryConfig() {
    return new IcebergConfig(
        ImmutableMap.of(
            IcebergConstants.CATALOG_BACKEND, "memory", IcebergConstants.WAREHOUSE, "/tmp/wh"));
  }
}
