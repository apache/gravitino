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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.Test;

public class TestPaimonCatalogPropertiesMetadata {

  private final PaimonCatalogPropertiesMetadata metadata = new PaimonCatalogPropertiesMetadata();

  @Test
  void testSensitivePropertiesAreHidden() {
    assertTrue(metadata.isHiddenProperty(PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_USER));
    assertTrue(metadata.isHiddenProperty(PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_PASSWORD));
    assertTrue(metadata.isHiddenProperty(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertTrue(metadata.isHiddenProperty(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
    assertTrue(metadata.isHiddenProperty(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID));
    assertTrue(metadata.isHiddenProperty(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET));
    // REST bearer token and DLF credentials share the same hidden requirement
    assertTrue(metadata.isHiddenProperty(PaimonConstants.TOKEN));
    assertTrue(metadata.isHiddenProperty(PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_ID));
    assertTrue(metadata.isHiddenProperty(PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_SECRET));
    assertTrue(metadata.isHiddenProperty(PaimonConstants.GRAVITINO_DLF_SECURITY_TOKEN));
  }

  @Test
  void testNonSensitivePropertiesAreNotHidden() {
    assertFalse(metadata.isHiddenProperty(PaimonCatalogPropertiesMetadata.WAREHOUSE));
    assertFalse(metadata.isHiddenProperty(PaimonCatalogPropertiesMetadata.URI));
    // DLF token metadata (provider type, path, loader) is not a credential — stays visible.
    assertFalse(metadata.isHiddenProperty(PaimonConstants.GRAVITINO_TOKEN_PROVIDER));
    assertFalse(metadata.isHiddenProperty(PaimonConstants.GRAVITINO_DLF_TOKEN_PATH));
    assertFalse(metadata.isHiddenProperty(PaimonConstants.GRAVITINO_DLF_TOKEN_LOADER));
  }
}
