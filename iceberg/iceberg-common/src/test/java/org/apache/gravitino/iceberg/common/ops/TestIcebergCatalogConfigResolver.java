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

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestIcebergCatalogConfigResolver {

  @Test
  void toIcebergConfigPromotesBypassPrefix() {
    Map<String, String> catalogProperties = new HashMap<>();
    catalogProperties.put(IcebergConstants.CATALOG_BACKEND, "jdbc");
    catalogProperties.put("gravitino.bypass.custom-k1", "custom-v1");
    catalogProperties.put("custom-k2", "custom-v2");

    IcebergConfig icebergConfig = IcebergCatalogConfigResolver.toIcebergConfig(catalogProperties);
    Map<String, String> props = icebergConfig.getIcebergCatalogProperties();
    Assertions.assertEquals("custom-v1", props.get("custom-k1"));
    Assertions.assertTrue(props.containsKey("custom-k2"));
  }

  @Test
  void resolvePropsRejectsNonIcebergProvider() {
    Catalog catalog = Mockito.mock(Catalog.class);
    Mockito.when(catalog.provider()).thenReturn("jdbc-mysql");
    Mockito.when(catalog.name()).thenReturn("mysql");
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> IcebergCatalogConfigResolver.resolveProps(catalog));
  }

  @Test
  void resolvePropsFromPlainCatalog() {
    Catalog catalog = Mockito.mock(Catalog.class);
    Mockito.when(catalog.provider()).thenReturn("lakehouse-iceberg");
    Mockito.when(catalog.name()).thenReturn("ice");
    Mockito.when(catalog.properties())
        .thenReturn(Map.of(IcebergConstants.CATALOG_BACKEND, "memory"));
    Mockito.when(catalog.supportsSecrets()).thenThrow(new UnsupportedOperationException());
    Mockito.when(catalog.supportsCredentials()).thenThrow(new UnsupportedOperationException());

    Map<String, String> props = IcebergCatalogConfigResolver.resolveProps(catalog);
    Assertions.assertEquals("memory", props.get(IcebergConstants.CATALOG_BACKEND));
  }
}
