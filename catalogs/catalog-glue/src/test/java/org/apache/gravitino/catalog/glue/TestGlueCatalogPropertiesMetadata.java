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
package org.apache.gravitino.catalog.glue;

import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForAlter;
import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForCreate;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_ACCESS_KEY_ID;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_GLUE_CATALOG_ID;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_GLUE_ENDPOINT;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_REGION;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_SECRET_ACCESS_KEY;
import static org.apache.gravitino.catalog.glue.GlueConstants.DEFAULT_TABLE_FORMAT;
import static org.apache.gravitino.catalog.glue.GlueConstants.DEFAULT_TABLE_FORMAT_FILTER;
import static org.apache.gravitino.catalog.glue.GlueConstants.DEFAULT_TABLE_FORMAT_VALUE;
import static org.apache.gravitino.catalog.glue.GlueConstants.TABLE_FORMAT_FILTER;
import static org.apache.gravitino.catalog.glue.GlueConstants.WAREHOUSE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.connector.HiddenPropertyMaskUtils;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestGlueCatalogPropertiesMetadata {

  private GlueCatalogPropertiesMetadata metadata;

  @BeforeEach
  void setUp() {
    metadata = new GlueCatalogPropertiesMetadata();
  }

  @Test
  void testAwsRegionIsRequired() {
    assertTrue(metadata.isRequiredProperty(AWS_REGION));
  }

  @Test
  void testWarehouseIsRequired() {
    assertTrue(metadata.isRequiredProperty(WAREHOUSE));
  }

  @Test
  void testAwsGlueCatalogIdIsOptional() {
    assertFalse(metadata.isRequiredProperty(AWS_GLUE_CATALOG_ID));
  }

  @Test
  void testAwsRegionIsImmutable() {
    assertTrue(metadata.isImmutableProperty(AWS_REGION));
  }

  @Test
  void testAwsGlueCatalogIdIsImmutable() {
    assertTrue(metadata.isImmutableProperty(AWS_GLUE_CATALOG_ID));
  }

  @Test
  void testCredentialsAreOptional() {
    assertFalse(metadata.isRequiredProperty(AWS_ACCESS_KEY_ID));
    assertFalse(metadata.isRequiredProperty(AWS_SECRET_ACCESS_KEY));
  }

  @Test
  void testCredentialsAreHidden() {
    assertTrue(metadata.isHiddenProperty(AWS_ACCESS_KEY_ID));
    assertTrue(metadata.isHiddenProperty(AWS_SECRET_ACCESS_KEY));
  }

  @Test
  void testEndpointIsOptionalAndNotHidden() {
    assertFalse(metadata.isRequiredProperty(AWS_GLUE_ENDPOINT));
    assertFalse(metadata.isHiddenProperty(AWS_GLUE_ENDPOINT));
  }

  @Test
  void testDefaultTableFormatDefaultValue() {
    assertEquals(
        DEFAULT_TABLE_FORMAT_VALUE,
        metadata.getDefaultValue(DEFAULT_TABLE_FORMAT),
        "Default table format should be 'hive'");
  }

  @Test
  void testTableFormatFilterDefaultValue() {
    assertEquals(
        DEFAULT_TABLE_FORMAT_FILTER,
        metadata.getDefaultValue(TABLE_FORMAT_FILTER),
        "Default table format filter should be 'all'");
  }

  @Test
  void testRejectsMistypedS3CredentialProperties() {
    Map<String, String> props =
        ImmutableMap.of(
            AWS_REGION,
            "us-east-1",
            WAREHOUSE,
            "s3://bucket/wh",
            S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
            "AKIATEST",
            S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
            "secret");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> validatePropertyForCreate(metadata, props));
    assertTrue(exception.getMessage().contains("Unknown properties"));
    assertTrue(exception.getMessage().contains(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
  }

  @Test
  void testAcceptsDeclaredAwsCredentialsAndMasksOnRead() {
    Map<String, String> props =
        ImmutableMap.of(
            AWS_REGION,
            "us-east-1",
            WAREHOUSE,
            "s3://bucket/wh",
            AWS_ACCESS_KEY_ID,
            "AKIATEST",
            AWS_SECRET_ACCESS_KEY,
            "secret");
    assertDoesNotThrow(() -> validatePropertyForCreate(metadata, props));

    Map<String, String> masked = HiddenPropertyMaskUtils.maskHiddenProperties(props, metadata);
    assertEquals(HiddenPropertyMaskUtils.MASKED_VALUE, masked.get(AWS_ACCESS_KEY_ID));
    assertEquals(HiddenPropertyMaskUtils.MASKED_VALUE, masked.get(AWS_SECRET_ACCESS_KEY));
  }

  @Test
  void testAlterRejectsMistypedS3CredentialUpsert() {
    Map<String, String> upserts =
        ImmutableMap.of(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, "secret");
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validatePropertyForAlter(metadata, upserts, Collections.emptyMap()));
    assertTrue(exception.getMessage().contains(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
  }

  @Test
  void testAlterAllowsRemovingMistypedS3Credential() {
    Map<String, String> deletes =
        ImmutableMap.of(
            S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
            S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY);
    assertDoesNotThrow(() -> validatePropertyForAlter(metadata, Collections.emptyMap(), deletes));
  }
}
