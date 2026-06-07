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
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.cache.LocalTableMetadataCache;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIcebergCatalogPropertiesMetadata {

  private IcebergCatalogPropertiesMetadata metadata;

  @BeforeEach
  void setUp() {
    metadata = new IcebergCatalogPropertiesMetadata();
  }

  @Test
  void testTableMetadataCacheImplDefaultValue() {
    Assertions.assertEquals(
        LocalTableMetadataCache.class.getName(),
        metadata.getDefaultValue(IcebergConstants.TABLE_METADATA_CACHE_IMPL));
    Assertions.assertEquals(
        IcebergConfig.TABLE_METADATA_CACHE_IMPL.getDefaultValue(),
        metadata.getDefaultValue(IcebergConstants.TABLE_METADATA_CACHE_IMPL));
  }

  @Test
  void testTableMetadataCacheCapacityDefaultValue() {
    Assertions.assertEquals(
        1000, metadata.getDefaultValue(IcebergConstants.TABLE_METADATA_CACHE_CAPACITY));
    Assertions.assertEquals(
        IcebergConfig.TABLE_METADATA_CACHE_CAPACITY.getDefaultValue(),
        metadata.getDefaultValue(IcebergConstants.TABLE_METADATA_CACHE_CAPACITY));
  }

  @Test
  void testTableMetadataCacheDefaultsViaGetOrDefault() {
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            IcebergCatalogPropertiesMetadata.CATALOG_BACKEND,
            "hive",
            IcebergCatalogPropertiesMetadata.URI,
            "thrift://127.0.0.1:9083",
            IcebergCatalogPropertiesMetadata.WAREHOUSE,
            "/tmp/warehouse");

    Assertions.assertEquals(
        IcebergConfig.TABLE_METADATA_CACHE_IMPL.getDefaultValue(),
        metadata.getOrDefault(catalogProperties, IcebergConstants.TABLE_METADATA_CACHE_IMPL));
    Assertions.assertEquals(
        IcebergConfig.TABLE_METADATA_CACHE_CAPACITY.getDefaultValue(),
        metadata.getOrDefault(catalogProperties, IcebergConstants.TABLE_METADATA_CACHE_CAPACITY));
  }

  @Test
  void testRESTCatalogBackendClientTimeoutDefaultValues() {
    Assertions.assertEquals(
        IcebergConfig.REST_CATALOG_BACKEND_CLIENT_CONNECTION_TIMEOUT_MS.getDefaultValue(),
        metadata.getDefaultValue(
            IcebergConstants.REST_CATALOG_BACKEND_CLIENT_CONNECTION_TIMEOUT_MS));
    Assertions.assertEquals(
        IcebergConfig.REST_CATALOG_BACKEND_CLIENT_SOCKET_TIMEOUT_MS.getDefaultValue(),
        metadata.getDefaultValue(IcebergConstants.REST_CATALOG_BACKEND_CLIENT_SOCKET_TIMEOUT_MS));
  }

  @Test
  void testRESTCatalogBackendClientTimeoutsViaGetOrDefault() {
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            IcebergCatalogPropertiesMetadata.CATALOG_BACKEND,
            "rest",
            IcebergCatalogPropertiesMetadata.URI,
            "http://127.0.0.1:9001/iceberg");

    Assertions.assertEquals(
        IcebergConfig.REST_CATALOG_BACKEND_CLIENT_CONNECTION_TIMEOUT_MS.getDefaultValue(),
        metadata.getOrDefault(
            catalogProperties, IcebergConstants.REST_CATALOG_BACKEND_CLIENT_CONNECTION_TIMEOUT_MS));
    Assertions.assertEquals(
        IcebergConfig.REST_CATALOG_BACKEND_CLIENT_SOCKET_TIMEOUT_MS.getDefaultValue(),
        metadata.getOrDefault(
            catalogProperties, IcebergConstants.REST_CATALOG_BACKEND_CLIENT_SOCKET_TIMEOUT_MS));

    catalogProperties =
        ImmutableMap.of(
            IcebergCatalogPropertiesMetadata.CATALOG_BACKEND,
            "rest",
            IcebergCatalogPropertiesMetadata.URI,
            "http://127.0.0.1:9001/iceberg",
            IcebergConstants.REST_CATALOG_BACKEND_CLIENT_CONNECTION_TIMEOUT_MS,
            "1234",
            IcebergConstants.REST_CATALOG_BACKEND_CLIENT_SOCKET_TIMEOUT_MS,
            "5678");

    Assertions.assertEquals(
        1234,
        metadata.getOrDefault(
            catalogProperties, IcebergConstants.REST_CATALOG_BACKEND_CLIENT_CONNECTION_TIMEOUT_MS));
    Assertions.assertEquals(
        5678,
        metadata.getOrDefault(
            catalogProperties, IcebergConstants.REST_CATALOG_BACKEND_CLIENT_SOCKET_TIMEOUT_MS));
  }

  @Test
  void testRESTCatalogBackendClientTimeoutPropertiesAreTransformed() {
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            IcebergConstants.REST_CATALOG_BACKEND_CLIENT_CONNECTION_TIMEOUT_MS,
            "1234",
            IcebergConstants.REST_CATALOG_BACKEND_CLIENT_SOCKET_TIMEOUT_MS,
            "5678");

    Map<String, String> transformedProperties = metadata.transformProperties(catalogProperties);

    Assertions.assertEquals(
        "1234",
        transformedProperties.get(IcebergConstants.ICEBERG_REST_CLIENT_CONNECTION_TIMEOUT_MS));
    Assertions.assertEquals(
        "5678", transformedProperties.get(IcebergConstants.ICEBERG_REST_CLIENT_SOCKET_TIMEOUT_MS));
  }
}
