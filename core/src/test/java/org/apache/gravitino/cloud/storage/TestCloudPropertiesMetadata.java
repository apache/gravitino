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
package org.apache.gravitino.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.config.CredentialConfig;
import org.apache.gravitino.secret.SecretPropertyUtils;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.gravitino.storage.COSProperties;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.Test;

public class TestCloudPropertiesMetadata {

  @Test
  void testS3CredentialConfigPropertiesAreDeclared() {
    var metadata = S3PropertiesMetadata.PROPERTY_ENTRIES;
    assertTrue(metadata.containsKey(S3Properties.GRAVITINO_S3_ENDPOINT));
    assertTrue(metadata.containsKey(S3Properties.GRAVITINO_S3_REGION));
    assertTrue(metadata.containsKey(S3Properties.GRAVITINO_S3_ROLE_ARN));
    assertFalse(metadata.get(S3Properties.GRAVITINO_S3_ENDPOINT).isHidden());
    assertFalse(metadata.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID).isHidden());
    assertTrue(metadata.get(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY).isHidden());
  }

  @Test
  void testOssCredentialConfigPropertiesAreDeclared() {
    var metadata = OSSPropertiesMetadata.PROPERTY_ENTRIES;
    assertTrue(metadata.containsKey(OSSProperties.GRAVITINO_OSS_ENDPOINT));
    assertTrue(metadata.containsKey(OSSProperties.GRAVITINO_OSS_ROLE_ARN));
    assertFalse(metadata.get(OSSProperties.GRAVITINO_OSS_REGION).isHidden());
  }

  @Test
  void testAzureAdlsPropertiesAreDeclared() {
    var metadata = AzurePropertiesMetadata.PROPERTY_ENTRIES;
    assertTrue(metadata.containsKey(AzureProperties.GRAVITINO_AZURE_TENANT_ID));
    assertTrue(metadata.containsKey(AzureProperties.GRAVITINO_AZURE_CLIENT_ID));
    assertFalse(metadata.get(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME).isHidden());
    assertTrue(metadata.get(AzureProperties.GRAVITINO_AZURE_CLIENT_SECRET).isHidden());
  }

  @Test
  void testCosCredentialConfigPropertiesAreDeclared() {
    var metadata = COSPropertiesMetadata.PROPERTY_ENTRIES;
    assertTrue(metadata.containsKey(COSProperties.GRAVITINO_COS_REGION));
    assertTrue(metadata.containsKey(COSProperties.GRAVITINO_COS_ENDPOINT));
    assertTrue(metadata.containsKey(COSProperties.GRAVITINO_COS_ROLE_ARN));
    assertTrue(metadata.containsKey(COSProperties.GRAVITINO_COS_EXTERNAL_ID));
    assertTrue(metadata.containsKey(COSProperties.GRAVITINO_COS_APP_ID));
    assertFalse(metadata.get(COSProperties.GRAVITINO_COS_ACCESS_KEY_ID).isHidden());
    assertTrue(metadata.get(COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET).isHidden());
  }

  @Test
  void testCredentialPropertyEntriesIncludeTokenExpireKeys() {
    var metadata = CredentialConfig.CREDENTIAL_PROPERTY_ENTRIES;
    assertTrue(metadata.containsKey(CredentialConstants.CREDENTIAL_PROVIDERS));
    assertTrue(metadata.containsKey(CredentialConstants.S3_TOKEN_EXPIRE_IN_SECS));
    assertTrue(metadata.containsKey(CredentialConstants.ADLS_TOKEN_EXPIRE_IN_SECS));
    assertTrue(metadata.containsKey(CredentialConstants.COS_TOKEN_EXPIRE_IN_SECS));
    assertFalse(metadata.get(CredentialConstants.S3_TOKEN_EXPIRE_IN_SECS).isHidden());
  }

  @Test
  void testDeclaredSensitiveNamedCredentialKeysAreNonHidden() {
    assertFalse(
        CredentialConfig.CREDENTIAL_PROPERTY_ENTRIES
            .get(CredentialConstants.CREDENTIAL_PROVIDERS)
            .isHidden());
    assertTrue(
        SecretPropertyUtils.isSensitivePropertyKey(CredentialConstants.CREDENTIAL_PROVIDERS));
    assertFalse(
        CredentialConfig.CREDENTIAL_PROPERTY_ENTRIES
            .get(CredentialConstants.S3_TOKEN_EXPIRE_IN_SECS)
            .isHidden());
    assertTrue(
        SecretPropertyUtils.isSensitivePropertyKey(CredentialConstants.S3_TOKEN_EXPIRE_IN_SECS));
    assertFalse(
        AzurePropertiesMetadata.PROPERTY_ENTRIES
            .get(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME)
            .isHidden());
    assertTrue(
        SecretPropertyUtils.isSensitivePropertyKey(
            AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME));
  }
}
