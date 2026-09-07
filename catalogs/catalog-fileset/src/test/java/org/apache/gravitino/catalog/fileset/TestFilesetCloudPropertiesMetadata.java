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
package org.apache.gravitino.catalog.fileset;

import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForCreate;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.connector.HiddenPropertyMaskUtils;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.gravitino.storage.COSProperties;
import org.apache.gravitino.storage.GCSProperties;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Fileset must select shared cloud credential definitions so API responses mask secrets instead of
 * treating them as ordinary undeclared properties.
 */
public class TestFilesetCloudPropertiesMetadata {

  static Object[][] filesetPropertiesMetadata() {
    return new Object[][] {
      {new FilesetCatalogPropertiesMetadata()},
      {new FilesetSchemaPropertiesMetadata()},
      {new FilesetPropertiesMetadata()}
    };
  }

  @ParameterizedTest
  @MethodSource("filesetPropertiesMetadata")
  void testCloudCredentialsAreHidden(PropertiesMetadata metadata) {
    assertTrue(metadata.isHiddenProperty(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertTrue(metadata.isHiddenProperty(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
    assertTrue(metadata.isHiddenProperty(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID));
    assertTrue(metadata.isHiddenProperty(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET));
    assertTrue(metadata.isHiddenProperty(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY));
    assertTrue(metadata.isHiddenProperty(COSProperties.GRAVITINO_COS_ACCESS_KEY_ID));
    assertTrue(metadata.isHiddenProperty(COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET));
  }

  @ParameterizedTest
  @MethodSource("filesetPropertiesMetadata")
  void testNonCredentialCloudPropertiesAreVisible(PropertiesMetadata metadata) {
    assertFalse(metadata.isHiddenProperty(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME));
    assertFalse(metadata.isHiddenProperty(GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE));
  }

  @Test
  void testApiResponseMasksFilesetCatalogS3Secret() {
    FilesetCatalogPropertiesMetadata metadata = new FilesetCatalogPropertiesMetadata();
    Map<String, String> properties =
        ImmutableMap.of(
            FilesetCatalogPropertiesMetadata.LOCATION,
            "s3a://bucket/path",
            S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
            "AKIATEST",
            S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
            "secret-value");

    Map<String, String> response =
        HiddenPropertyMaskUtils.maskHiddenProperties(properties, metadata);

    assertEquals("s3a://bucket/path", response.get(FilesetCatalogPropertiesMetadata.LOCATION));
    assertEquals(
        HiddenPropertyMaskUtils.MASKED_VALUE,
        response.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertEquals(
        HiddenPropertyMaskUtils.MASKED_VALUE,
        response.get(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
  }

  @Test
  void testAcceptsDeclaredS3CredentialsOnCreate() {
    FilesetCatalogPropertiesMetadata metadata = new FilesetCatalogPropertiesMetadata();
    Map<String, String> properties =
        ImmutableMap.of(
            FilesetCatalogPropertiesMetadata.LOCATION,
            "s3a://bucket/path",
            S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
            "AKIATEST",
            S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
            "secret-value");
    assertDoesNotThrow(() -> validatePropertyForCreate(metadata, properties));
  }

  @Test
  void testRejectsUnknownCatalogProperty() {
    FilesetCatalogPropertiesMetadata metadata = new FilesetCatalogPropertiesMetadata();
    Map<String, String> properties = ImmutableMap.of("foo-bar", "value");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> validatePropertyForCreate(metadata, properties));
    assertTrue(exception.getMessage().contains("Unknown properties"));
    assertTrue(exception.getMessage().contains("foo-bar"));
  }

  @Test
  void testAcceptsFsPathConfigPrefix() {
    FilesetCatalogPropertiesMetadata metadata = new FilesetCatalogPropertiesMetadata();
    Map<String, String> properties =
        ImmutableMap.of(
            FilesetCatalogPropertiesMetadata.FS_GRAVITINO_PATH_CONFIG_PREFIX + "cluster1",
            "hdfs://cluster1/",
            FilesetCatalogPropertiesMetadata.FS_GRAVITINO_PATH_CONFIG_PREFIX
                + "cluster1.config.resource",
            "/etc/core-site.xml");
    assertDoesNotThrow(() -> validatePropertyForCreate(metadata, properties));
  }
}
