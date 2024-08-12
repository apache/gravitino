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
package org.apache.gravitino.iceberg.common.ops;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.UUID;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestConfigBasedIcebergTableOpsProvider {
  private static final String STORE_PATH =
      "/tmp/gravitino_test_iceberg_jdbc_backend_" + UUID.randomUUID().toString().replace("-", "");

  @ParameterizedTest
  @ValueSource(
      strings = {"hive_backend", "jdbc_backend", IcebergConstants.GRAVITINO_DEFAULT_CATALOG})
  public void testValidIcebergTableOps(String catalogName) {
    Map<String, String> config = Maps.newHashMap();

    if ("hive_backend".equals(catalogName)) {
      config.put(String.format("catalog.%s.catalog-backend-name", catalogName), catalogName);
      config.put(String.format("catalog.%s.catalog-backend", catalogName), "hive");
      config.put(String.format("catalog.%s.uri", catalogName), "thrift://127.0.0.1:9083");
      config.put(String.format("catalog.%s.warehouse", catalogName), "/tmp/usr/hive/warehouse");
    } else if ("jdbc_backend".equals(catalogName)) {
      config.put(String.format("catalog.%s.catalog-backend-name", catalogName), catalogName);
      config.put(String.format("catalog.%s.catalog-backend", catalogName), "jdbc");
      config.put(
          String.format("catalog.%s.uri", catalogName),
          String.format("jdbc:h2:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", STORE_PATH));
      config.put(String.format("catalog.%s.warehouse", catalogName), "/tmp/usr/jdbc/warehouse");
      config.put(String.format("catalog.%s.jdbc.password", catalogName), "gravitino");
      config.put(String.format("catalog.%s.jdbc.user", catalogName), "gravitino");
      config.put(String.format("catalog.%s.jdbc-driver", catalogName), "org.h2.Driver");
      config.put(String.format("catalog.%s.jdbc-initialize", catalogName), "true");
    } else if (IcebergConstants.GRAVITINO_DEFAULT_CATALOG.equals(catalogName)) {
      config.put("catalog-backend-name", catalogName);
    }
    ConfigBasedIcebergTableOpsProvider provider = new ConfigBasedIcebergTableOpsProvider();
    provider.initialize(config);

    IcebergTableOps ops = provider.getIcebergTableOps(catalogName);

    Assertions.assertEquals(catalogName, ops.catalog.name());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "not_match"})
  public void testInvalidIcebergTableOps(String catalogName) {
    ConfigBasedIcebergTableOpsProvider provider = new ConfigBasedIcebergTableOpsProvider();
    provider.initialize(Maps.newHashMap());

    Assertions.assertThrowsExactly(
        RuntimeException.class, () -> provider.getIcebergTableOps(catalogName));
  }
}
