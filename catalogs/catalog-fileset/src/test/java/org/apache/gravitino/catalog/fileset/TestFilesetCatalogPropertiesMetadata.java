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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.Test;

class TestFilesetCatalogPropertiesMetadata {

  private final FilesetCatalogPropertiesMetadata catalogMetadata =
      new FilesetCatalogPropertiesMetadata();
  private final FilesetSchemaPropertiesMetadata schemaMetadata =
      new FilesetSchemaPropertiesMetadata();
  private final FilesetPropertiesMetadata filesetMetadata = new FilesetPropertiesMetadata();

  @Test
  void testStorageCredentialPropertiesAreHidden() {
    assertCredentialPropertiesHidden(catalogMetadata);
    assertCredentialPropertiesHidden(schemaMetadata);
    assertCredentialPropertiesHidden(filesetMetadata);
  }

  @Test
  void testNonCredentialStoragePropertiesAreNotHidden() {
    assertNonCredentialPropertiesVisible(catalogMetadata);
    assertNonCredentialPropertiesVisible(schemaMetadata);
    assertNonCredentialPropertiesVisible(filesetMetadata);
  }

  private void assertNonCredentialPropertiesVisible(PropertiesMetadata metadata) {
    // Connection/identity information that is not sensitive should remain visible.
    assertFalse(metadata.isHiddenProperty(S3Properties.GRAVITINO_S3_ENDPOINT));
    assertFalse(metadata.isHiddenProperty(S3Properties.GRAVITINO_S3_REGION));
    assertFalse(metadata.isHiddenProperty(OSSProperties.GRAVITINO_OSS_ENDPOINT));
  }

  private void assertCredentialPropertiesHidden(PropertiesMetadata metadata) {
    assertTrue(metadata.isHiddenProperty(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertTrue(metadata.isHiddenProperty(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
    assertTrue(metadata.isHiddenProperty(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID));
    assertTrue(metadata.isHiddenProperty(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET));
    assertTrue(metadata.isHiddenProperty(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME));
    assertTrue(metadata.isHiddenProperty(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY));
  }
}
