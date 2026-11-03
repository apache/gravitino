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

import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_ACCESS_KEY_ID;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_GLUE_API_CALL_ATTEMPT_TIMEOUT_MS;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_GLUE_API_CALL_TIMEOUT_MS;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_GLUE_CATALOG_ID;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_GLUE_ENDPOINT;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_GLUE_MAX_ATTEMPTS;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_REGION;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_SECRET_ACCESS_KEY;
import static org.apache.gravitino.catalog.glue.GlueConstants.DEFAULT_TABLE_FORMAT;
import static org.apache.gravitino.catalog.glue.GlueConstants.DEFAULT_TABLE_FORMAT_FILTER;
import static org.apache.gravitino.catalog.glue.GlueConstants.DEFAULT_TABLE_FORMAT_VALUE;
import static org.apache.gravitino.catalog.glue.GlueConstants.TABLE_FORMAT_FILTER;
import static org.apache.gravitino.catalog.glue.GlueConstants.WAREHOUSE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
<<<<<<< HEAD
=======
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
>>>>>>> 30a11bb0c ([#14516] fix(catalog-glue): Map AWS errors to client-facing statuses and validate catalog properties (#14517))
import static org.junit.jupiter.api.Assertions.assertTrue;

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
  void testRegionMustBeKnownLowerCaseRegion() {
    decode(AWS_REGION, "us-east-2");
    assertInvalid(AWS_REGION, "us-east-9");
    assertInvalid(AWS_REGION, "US-EAST-2");
    assertInvalid(AWS_REGION, "us-east-2 ");
    assertInvalid(AWS_REGION, "");
  }

  @Test
  void testCatalogIdMustBeTwelveDigits() {
    decode(AWS_GLUE_CATALOG_ID, "123456789012");
    assertInvalid(AWS_GLUE_CATALOG_ID, "12345678901");
    assertInvalid(AWS_GLUE_CATALOG_ID, "notanumber");
  }

  @Test
  void testEndpointMustBeHttpUrl() {
    decode(AWS_GLUE_ENDPOINT, "http://localhost:4566");
    decode(AWS_GLUE_ENDPOINT, "https://glue.us-east-2.amazonaws.com");
    decode(AWS_GLUE_ENDPOINT, "http://moto_server:5000");
    assertInvalid(AWS_GLUE_ENDPOINT, "http://:4566");
    assertInvalid(AWS_GLUE_ENDPOINT, "http://user@");
    assertInvalid(AWS_GLUE_ENDPOINT, "not a url");
    assertInvalid(AWS_GLUE_ENDPOINT, "ht!tp://bad");
    assertInvalid(AWS_GLUE_ENDPOINT, "glue.us-east-2.amazonaws.com");
  }

  @Test
  void testWarehouseMustBeUriWithScheme() {
    decode(WAREHOUSE, "s3://bucket/warehouse");
    decode(WAREHOUSE, "s3a://bucket");
    // Hive-only catalogs may keep their tables on other file systems.
    decode(WAREHOUSE, "hdfs://nn:8020/user/hive/warehouse");
    decode(WAREHOUSE, "file:///tmp/warehouse");
    assertInvalid(WAREHOUSE, "/tmp/warehouse");
    assertInvalid(WAREHOUSE, "s3://");
    assertInvalid(WAREHOUSE, "");
  }

  @Test
  void testTableFormatValuesMustBeDocumented() {
    decode(DEFAULT_TABLE_FORMAT, "ICEBERG");
    decode(DEFAULT_TABLE_FORMAT, "hive");
    assertInvalid(DEFAULT_TABLE_FORMAT, "paimon");
    assertInvalid(DEFAULT_TABLE_FORMAT, "");

    decode(TABLE_FORMAT_FILTER, "iceberg, hive");
    decode(TABLE_FORMAT_FILTER, "all");
    assertInvalid(TABLE_FORMAT_FILTER, "icebrg");
    assertInvalid(TABLE_FORMAT_FILTER, "iceberg,");
    assertInvalid(TABLE_FORMAT_FILTER, "");
  }

  @Test
  void testTimeoutAndRetryValuesMustBePositive() {
    assertEquals(5000L, decode(AWS_GLUE_API_CALL_TIMEOUT_MS, "5000"));
    assertEquals(3, decode(AWS_GLUE_MAX_ATTEMPTS, "3"));
    assertInvalid(AWS_GLUE_API_CALL_TIMEOUT_MS, "0");
    assertInvalid(AWS_GLUE_API_CALL_ATTEMPT_TIMEOUT_MS, "-1");
    assertInvalid(AWS_GLUE_MAX_ATTEMPTS, "zero");
  }

  private Object decode(String key, String value) {
    return metadata.getPropertyEntry(key).decode(value);
  }

  private void assertInvalid(String key, String value) {
    assertThrows(
        IllegalArgumentException.class,
        () -> decode(key, value),
        () -> key + " should reject '" + value + "'");
  }
}
