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

import org.apache.gravitino.catalog.lakehouse.paimon.storage.PaimonOSSFileSystemConfig;
import org.apache.gravitino.catalog.lakehouse.paimon.storage.PaimonS3FileSystemConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestPaimonCatalogPropertiesMetadata {

  private PaimonCatalogPropertiesMetadata metadata;

  @BeforeEach
  public void setUp() {
    metadata = new PaimonCatalogPropertiesMetadata();
  }

  @Test
  public void testJdbcPasswordIsHidden() {
    assertTrue(metadata.isHiddenProperty(PaimonConstants.GRAVITINO_JDBC_PASSWORD));
  }

  @Test
  public void testBearerTokenIsHidden() {
    assertTrue(metadata.isHiddenProperty(PaimonConstants.TOKEN));
  }

  @Test
  public void testDlfAccessKeySecretIsHidden() {
    assertTrue(metadata.isHiddenProperty(PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_SECRET));
  }

  @Test
  public void testDlfSecurityTokenIsHidden() {
    assertTrue(metadata.isHiddenProperty(PaimonConstants.GRAVITINO_DLF_SECURITY_TOKEN));
  }

  @Test
  public void testS3SecretKeyIsHidden() {
    assertTrue(metadata.isHiddenProperty(PaimonS3FileSystemConfig.S3_SECRET_KEY));
  }

  @Test
  public void testOssSecretKeyIsHidden() {
    assertTrue(metadata.isHiddenProperty(PaimonOSSFileSystemConfig.OSS_SECRET_KEY));
  }

  @Test
  public void testNonSecretPropertiesAreNotHidden() {
    assertFalse(metadata.isHiddenProperty(PaimonConstants.GRAVITINO_JDBC_USER));
    assertFalse(metadata.isHiddenProperty(PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_ID));
    assertFalse(metadata.isHiddenProperty(PaimonS3FileSystemConfig.S3_ACCESS_KEY));
    assertFalse(metadata.isHiddenProperty(PaimonOSSFileSystemConfig.OSS_ACCESS_KEY));
  }
}
