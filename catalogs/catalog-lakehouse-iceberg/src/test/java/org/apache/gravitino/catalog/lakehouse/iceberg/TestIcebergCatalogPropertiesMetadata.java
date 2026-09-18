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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.PropertiesMetadataHelpers;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.cache.LocalTableMetadataCache;
import org.apache.gravitino.storage.COSProperties;
import org.apache.gravitino.storage.S3Properties;
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
  void testJdbcUserIsVisibleAndPasswordIsHidden() {
    Assertions.assertFalse(
        metadata.isHiddenProperty(IcebergCatalogPropertiesMetadata.GRAVITINO_JDBC_USER));
    Assertions.assertTrue(
        metadata.isHiddenProperty(IcebergCatalogPropertiesMetadata.GRAVITINO_JDBC_PASSWORD));
  }

  @Test
  void testCloudAccessKeyIdsAreVisibleAndSecretsAreHidden() {
    Assertions.assertFalse(metadata.isHiddenProperty(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    Assertions.assertTrue(metadata.isHiddenProperty(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
    Assertions.assertFalse(metadata.isHiddenProperty(COSProperties.GRAVITINO_COS_ACCESS_KEY_ID));
    Assertions.assertTrue(metadata.isHiddenProperty(COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET));
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

  @Test
  void testTableFormatVersionPropertiesAreMutableWithCodeDefaults() {
    for (String property :
        new String[] {
          IcebergConstants.TABLE_FORMAT_VERSION_DEFAULT, IcebergConstants.TABLE_FORMAT_VERSION_MAX
        }) {
      Assertions.assertTrue(metadata.containsProperty(property));
      Assertions.assertFalse(metadata.isImmutableProperty(property));
      Assertions.assertFalse(metadata.isHiddenProperty(property));
      Assertions.assertFalse(metadata.isRequiredProperty(property));
    }
    Assertions.assertEquals(
        IcebergTablePropertiesMetadata.ICEBERG_DEFAULT_FORMAT_VERSION,
        metadata.getDefaultValue(IcebergConstants.TABLE_FORMAT_VERSION_DEFAULT));
    Assertions.assertEquals(
        IcebergConstants.DEFAULT_MAX_TABLE_FORMAT_VERSION,
        metadata.getDefaultValue(IcebergConstants.TABLE_FORMAT_VERSION_MAX));

    // Both reach the Iceberg catalog configuration on the Gravitino API.
    Map<String, String> transformed =
        metadata.transformProperties(
            ImmutableMap.of(
                IcebergConstants.TABLE_FORMAT_VERSION_DEFAULT,
                "3",
                IcebergConstants.TABLE_FORMAT_VERSION_MAX,
                "3"));
    Assertions.assertEquals("3", transformed.get(IcebergConstants.TABLE_FORMAT_VERSION_DEFAULT));
    Assertions.assertEquals("3", transformed.get(IcebergConstants.TABLE_FORMAT_VERSION_MAX));
  }

  @Test
  void testInvalidTableFormatVersionValuesAreRejected() {
    for (String property :
        new String[] {
          IcebergConstants.TABLE_FORMAT_VERSION_DEFAULT, IcebergConstants.TABLE_FORMAT_VERSION_MAX
        }) {
      for (String invalid : new String[] {"0", "5", "abc", ""}) {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                PropertiesMetadataHelpers.validatePropertyForCreate(
                    metadata, withBackend(ImmutableMap.of(property, invalid))),
            property + "=" + invalid);
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                PropertiesMetadataHelpers.validatePropertyForAlter(
                    metadata, ImmutableMap.of(property, invalid), Collections.emptyMap()),
            property + "=" + invalid);
      }
      Assertions.assertDoesNotThrow(
          () ->
              PropertiesMetadataHelpers.validatePropertyForCreate(
                  metadata, withBackend(ImmutableMap.of(property, "3"))));
      Assertions.assertDoesNotThrow(
          () ->
              PropertiesMetadataHelpers.validatePropertyForAlter(
                  metadata, ImmutableMap.of(property, "3"), Collections.emptyMap()));
    }
  }

  /**
   * The bounds are checked against each other, and against Iceberg's own {@code
   * table-default.format-version} in any spelling, when the catalog is created or altered rather
   * than only when it loads.
   */
  @Test
  void testTableFormatVersionPropertiesAreValidatedTogetherOnCreateAndAlter() {
    IllegalArgumentException onCreate =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                PropertiesMetadataHelpers.validatePropertyForCreate(
                    metadata,
                    withBackend(
                        ImmutableMap.of(
                            IcebergConstants.TABLE_FORMAT_VERSION_DEFAULT,
                            "4",
                            IcebergConstants.TABLE_FORMAT_VERSION_MAX,
                            "3"))));
    Assertions.assertTrue(onCreate.getMessage().contains("must not exceed"), onCreate.getMessage());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            PropertiesMetadataHelpers.validatePropertyForCreate(
                metadata,
                withBackend(
                    ImmutableMap.of(
                        IcebergConstants.TABLE_FORMAT_VERSION_DEFAULT,
                        "3",
                        "gravitino.bypass." + IcebergConstants.ICEBERG_TABLE_DEFAULT_FORMAT_VERSION,
                        "2"))));
    Assertions.assertDoesNotThrow(
        () ->
            PropertiesMetadataHelpers.validatePropertyForCreate(
                metadata,
                withBackend(
                    ImmutableMap.of(
                        IcebergConstants.TABLE_FORMAT_VERSION_DEFAULT,
                        "3",
                        IcebergConstants.TABLE_FORMAT_VERSION_MAX,
                        "3"))));

    // An alter is checked against the properties the catalog would have after it.
    Map<String, String> current =
        withBackend(ImmutableMap.of(IcebergConstants.TABLE_FORMAT_VERSION_DEFAULT, "4"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            PropertiesMetadataHelpers.validatePropertyForAlter(
                metadata,
                current,
                ImmutableMap.of(IcebergConstants.TABLE_FORMAT_VERSION_MAX, "3"),
                Collections.emptyMap()));
    Assertions.assertDoesNotThrow(
        () ->
            PropertiesMetadataHelpers.validatePropertyForAlter(
                metadata,
                current,
                ImmutableMap.of(IcebergConstants.TABLE_FORMAT_VERSION_MAX, "3"),
                ImmutableMap.of(IcebergConstants.TABLE_FORMAT_VERSION_DEFAULT, "4")));
  }

  private static Map<String, String> withBackend(Map<String, String> properties) {
    Map<String, String> result = new HashMap<>(properties);
    result.put(IcebergCatalogPropertiesMetadata.CATALOG_BACKEND, "memory");
    result.put(IcebergCatalogPropertiesMetadata.URI, "memory://");
    return result;
  }
}
