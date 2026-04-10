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

import static org.apache.gravitino.catalog.glue.GlueConstants.METADATA_LOCATION;
import static org.apache.gravitino.catalog.glue.GlueConstants.TABLE_FORMAT_TYPE;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestGlueTablePropertiesMetadata {

  private GlueTablePropertiesMetadata metadata;

  @BeforeEach
  void setUp() {
    metadata = new GlueTablePropertiesMetadata();
  }

  @Test
  void testTableFormatTypeIsOptional() {
    assertFalse(metadata.isRequiredProperty(TABLE_FORMAT_TYPE));
  }

  @Test
  void testTableFormatTypeIsNotHidden() {
    assertFalse(metadata.isHiddenProperty(TABLE_FORMAT_TYPE));
  }

  @Test
  void testMetadataLocationIsOptional() {
    assertFalse(metadata.isRequiredProperty(METADATA_LOCATION));
  }

  @Test
  void testMetadataLocationIsNotHidden() {
    assertFalse(metadata.isHiddenProperty(METADATA_LOCATION));
  }
}
