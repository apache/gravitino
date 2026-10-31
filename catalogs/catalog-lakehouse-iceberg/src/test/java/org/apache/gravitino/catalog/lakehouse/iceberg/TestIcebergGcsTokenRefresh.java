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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergGcsTokenRefresh {

  @Test
  void reusesClientWhenTokenExpiryIsAbsent() {
    IcebergBackend backend = IcebergBackend.create(memoryConfig(null));
    IcebergCatalogWrapper wrapper = backend.wrapper();
    Assertions.assertSame(wrapper, backend.wrapper());
    backend.close();
  }

  @Test
  void reusesClientWhenTokenIsFarFromExpiry() {
    long expiresAt = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);
    IcebergBackend backend = IcebergBackend.create(memoryConfig(expiresAt));
    IcebergCatalogWrapper wrapper = backend.wrapper();
    Assertions.assertSame(wrapper, backend.wrapper());
    backend.close();
  }

  @Test
  void rebuildsClientInsideTheRefreshBuffer() {
    long expiresAt = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1);
    IcebergBackend backend = IcebergBackend.create(memoryConfig(expiresAt));
    IcebergCatalogWrapper first = backend.wrapper();
    Assertions.assertNotSame(first, backend.wrapper());
    backend.close();
  }

  private static Map<String, String> memoryConfig(Long expiresAt) {
    ImmutableMap.Builder<String, String> builder =
        ImmutableMap.<String, String>builder()
            .put(IcebergConstants.CATALOG_BACKEND, "memory")
            .put(IcebergConstants.WAREHOUSE, "/tmp/wh");
    if (expiresAt != null) {
      builder.put(IcebergConstants.ICEBERG_GCS_OAUTH2_TOKEN_EXPIRES_AT, String.valueOf(expiresAt));
    }
    return builder.build();
  }
}
