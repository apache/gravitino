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
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.iceberg.catalog.Namespace;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergEmbeddedRestBackend {

  @Test
  void auxModeUsesIrcWrapperForCatalogName() {
    IcebergCatalogWrapper ircWrapper = new IcebergCatalogWrapper(memoryConfig());

    Map<String, String> conf =
        ImmutableMap.of(
            IcebergConstants.CATALOG_BACKEND,
            "rest",
            IcebergConstants.URI,
            "http://remote.example:9001/iceberg",
            IcebergConstants.WAREHOUSE,
            "not-the-catalog-name");

    IcebergBackend backend =
        IcebergBackend.create(
            "orders",
            "metalake",
            conf,
            catalogName -> {
              if ("orders".equals(catalogName)) {
                return ircWrapper;
              }
              throw new NoSuchCatalogException("missing %s", catalogName);
            });
    Assertions.assertSame(ircWrapper, backend.wrapper());
    Assertions.assertSame(ircWrapper, backend.wrapper());
    backend.close();
    Assertions.assertDoesNotThrow(() -> ircWrapper.listNamespace(Namespace.empty()));
    Assertions.assertDoesNotThrow(ircWrapper::close);
  }

  @Test
  void auxModeUsesIrcWrapperForHiveBackend() {
    IcebergCatalogWrapper ircWrapper = new IcebergCatalogWrapper(memoryConfig());

    Map<String, String> conf =
        ImmutableMap.of(
            IcebergConstants.CATALOG_BACKEND, "hive", IcebergConstants.WAREHOUSE, "/tmp/wh");
    IcebergBackend backend =
        IcebergBackend.create("hive_cat", "metalake", conf, catalogName -> ircWrapper);
    Assertions.assertSame(ircWrapper, backend.wrapper());
    backend.close();
    Assertions.assertDoesNotThrow(ircWrapper::close);
  }

  @Test
  void withoutAuxBridgeCatalogOwnsItsClient() {
    Map<String, String> conf =
        ImmutableMap.of(
            IcebergConstants.CATALOG_BACKEND, "memory", IcebergConstants.WAREHOUSE, "/tmp/wh");
    IcebergBackend backend = IcebergBackend.create("mem", "metalake", conf);
    IcebergCatalogWrapper owned = backend.wrapper();
    Assertions.assertNotNull(owned);
    backend.close();
  }

  private static IcebergConfig memoryConfig() {
    return new IcebergConfig(
        ImmutableMap.of(
            IcebergConstants.CATALOG_BACKEND, "memory", IcebergConstants.WAREHOUSE, "/tmp/wh"));
  }
}
