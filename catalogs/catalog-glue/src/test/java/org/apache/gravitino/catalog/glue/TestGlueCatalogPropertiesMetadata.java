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
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_GLUE_CATALOG_ID;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_GLUE_ENDPOINT;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_REGION;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_SECRET_ACCESS_KEY;
import static org.apache.gravitino.catalog.glue.GlueConstants.DEFAULT_TABLE_FORMAT;
import static org.apache.gravitino.catalog.glue.GlueConstants.DEFAULT_TABLE_FORMAT_VALUE;
import static org.apache.gravitino.catalog.glue.GlueConstants.DEFAULT_TABLE_TYPE_FILTER;
import static org.apache.gravitino.catalog.glue.GlueConstants.TABLE_TYPE_FILTER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
  void testCredentialsAreHidden() {
    assertTrue(metadata.isHiddenProperty(AWS_ACCESS_KEY_ID));
    assertTrue(metadata.isHiddenProperty(AWS_SECRET_ACCESS_KEY));
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
  void testTableTypeFilterDefaultValue() {
    assertEquals(
        DEFAULT_TABLE_TYPE_FILTER,
        metadata.getDefaultValue(TABLE_TYPE_FILTER),
        "Default table type filter should be 'all'");
  }
}
