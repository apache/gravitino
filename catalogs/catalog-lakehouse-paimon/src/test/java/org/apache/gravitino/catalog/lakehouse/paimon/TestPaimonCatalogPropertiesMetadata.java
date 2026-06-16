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
package org.apache.gravitino.catalog.lakehouse.paimon;

import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestPaimonCatalogPropertiesMetadata {

  private PaimonCatalogPropertiesMetadata metadata;

  @BeforeEach
  void setUp() {
    metadata = new PaimonCatalogPropertiesMetadata();
  }

  @Test
  void testSensitivePropertiesAreHidden() {
    // JDBC credentials must not be returned in plaintext via REST API responses.
    Assertions.assertTrue(
        metadata.isHiddenProperty(PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_USER));
    Assertions.assertTrue(
        metadata.isHiddenProperty(PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_PASSWORD));

    // S3 credentials, sourced from the shared S3PropertiesMetadata, must be hidden.
    Assertions.assertTrue(metadata.isHiddenProperty(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    Assertions.assertTrue(metadata.isHiddenProperty(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));

    // OSS credentials, sourced from the shared OSSPropertiesMetadata, must be hidden.
    Assertions.assertTrue(metadata.isHiddenProperty(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID));
    Assertions.assertTrue(metadata.isHiddenProperty(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET));

    // REST catalog / Aliyun DLF credentials must be hidden.
    Assertions.assertTrue(metadata.isHiddenProperty(PaimonConstants.TOKEN));
    Assertions.assertTrue(metadata.isHiddenProperty(PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_ID));
    Assertions.assertTrue(
        metadata.isHiddenProperty(PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_SECRET));
    Assertions.assertTrue(metadata.isHiddenProperty(PaimonConstants.GRAVITINO_DLF_SECURITY_TOKEN));
  }

  @Test
  void testNonSensitivePropertiesAreNotHidden() {
    Assertions.assertFalse(metadata.isHiddenProperty(PaimonCatalogPropertiesMetadata.WAREHOUSE));
    Assertions.assertFalse(metadata.isHiddenProperty(PaimonCatalogPropertiesMetadata.URI));
    // Non-secret DLF token metadata (provider type, path, loader) stays visible.
    Assertions.assertFalse(metadata.isHiddenProperty(PaimonConstants.GRAVITINO_TOKEN_PROVIDER));
    Assertions.assertFalse(metadata.isHiddenProperty(PaimonConstants.GRAVITINO_DLF_TOKEN_PATH));
    Assertions.assertFalse(metadata.isHiddenProperty(PaimonConstants.GRAVITINO_DLF_TOKEN_LOADER));
  }
}
