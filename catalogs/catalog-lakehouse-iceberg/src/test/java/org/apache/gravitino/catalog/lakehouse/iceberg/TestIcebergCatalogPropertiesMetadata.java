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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.cache.LocalTableMetadataCache;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIcebergCatalogPropertiesMetadata {

  private IcebergCatalogPropertiesMetadata metadata;

  @BeforeEach
  public void setUp() {
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
  public void testJdbcPasswordIsHidden() {
    assertTrue(metadata.isHiddenProperty(IcebergCatalogPropertiesMetadata.GRAVITINO_JDBC_PASSWORD));
  }

  @Test
  public void testS3SecretAccessKeyIsHidden() {
    assertTrue(metadata.isHiddenProperty(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
  }

  @Test
  public void testOssAccessKeySecretIsHidden() {
    assertTrue(metadata.isHiddenProperty(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET));
  }

  @Test
  public void testAzureStorageAccountKeyIsHidden() {
    assertTrue(metadata.isHiddenProperty(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY));
  }

  @Test
  public void testNonSecretPropertiesAreNotHidden() {
    assertFalse(metadata.isHiddenProperty(IcebergCatalogPropertiesMetadata.GRAVITINO_JDBC_USER));
    assertFalse(metadata.isHiddenProperty(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertFalse(metadata.isHiddenProperty(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID));
  }
}
