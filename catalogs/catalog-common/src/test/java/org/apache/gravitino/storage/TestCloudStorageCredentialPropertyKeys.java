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
package org.apache.gravitino.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Set;
import org.apache.gravitino.catalog.glue.GlueConstants;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.junit.jupiter.api.Test;

public class TestCloudStorageCredentialPropertyKeys {

  @Test
  void testOmitStaticCredentialProperties() {
    Map<String, String> input =
        Map.of(
            S3Properties.GRAVITINO_S3_ENDPOINT,
            "https://s3.amazonaws.com",
            S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
            "AKIATEST",
            S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
            "secret",
            OSSProperties.GRAVITINO_OSS_REGION,
            "cn-hangzhou",
            "masked-any-key",
            CloudStorageCredentialPropertyKeys.MASKED_PROPERTY_VALUE);

    Map<String, String> filtered =
        CloudStorageCredentialPropertyKeys.omitStaticCredentialProperties(input);

    assertEquals("https://s3.amazonaws.com", filtered.get(S3Properties.GRAVITINO_S3_ENDPOINT));
    assertEquals("cn-hangzhou", filtered.get(OSSProperties.GRAVITINO_OSS_REGION));
    // Access key IDs and secrets are stripped from properties(); recover via getSecrets /
    // getCredentials.
    assertFalse(filtered.containsKey(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertFalse(filtered.containsKey(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
    assertFalse(filtered.containsKey("masked-any-key"));
  }

  @Test
  void testOmitMaskedAccessKeyIds() {
    Map<String, String> input =
        Map.of(
            S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
            CloudStorageCredentialPropertyKeys.MASKED_PROPERTY_VALUE,
            S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
            "secret");

    Map<String, String> filtered =
        CloudStorageCredentialPropertyKeys.omitStaticCredentialProperties(input);

    assertFalse(filtered.containsKey(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertFalse(filtered.containsKey(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
  }

  @Test
  void testStaticCredentialKeyDetection() {
    assertTrue(
        CloudStorageCredentialPropertyKeys.isStaticCredentialKey(
            COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET));
    assertTrue(
        CloudStorageCredentialPropertyKeys.isStaticCredentialKey(
            COSProperties.GRAVITINO_COS_ACCESS_KEY_ID));
    assertFalse(
        CloudStorageCredentialPropertyKeys.isStaticCredentialKey(
            COSProperties.GRAVITINO_COS_REGION));

    assertTrue(
        CloudStorageCredentialPropertyKeys.isStaticCredentialKey(
            AzureProperties.GRAVITINO_AZURE_CLIENT_SECRET));

    // Glue/Paimon-DLF secrets are catalog/metastore-connection credentials, not cloud-storage
    // secrets in the fileset properties map this filter governs.
    assertFalse(
        CloudStorageCredentialPropertyKeys.isStaticCredentialKey(
            GlueConstants.AWS_SECRET_ACCESS_KEY));
    assertFalse(
        CloudStorageCredentialPropertyKeys.isStaticCredentialKey(
            PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_SECRET));
    assertFalse(
        CloudStorageCredentialPropertyKeys.isStaticCredentialKey(
            PaimonConstants.GRAVITINO_DLF_SECURITY_TOKEN));
    assertFalse(
        CloudStorageCredentialPropertyKeys.isStaticCredentialKey(GlueConstants.AWS_ACCESS_KEY_ID));
    assertFalse(
        CloudStorageCredentialPropertyKeys.isStaticCredentialKey(
            PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_ID));
  }

  @Test
  void testAzureClientSecretStrippedButGlueAccessKeyIdsSurvive() {
    Map<String, String> input =
        Map.of(
            AzureProperties.GRAVITINO_AZURE_CLIENT_SECRET, "aad-secret",
            GlueConstants.AWS_ACCESS_KEY_ID, "ak",
            PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_ID, "dlf-ak");

    Map<String, String> filtered =
        CloudStorageCredentialPropertyKeys.omitStaticCredentialProperties(input);

    assertFalse(filtered.containsKey(AzureProperties.GRAVITINO_AZURE_CLIENT_SECRET));
    // Glue/DLF IDs are not in the fileset static cloud set; unmasked values survive here.
    assertEquals("ak", filtered.get(GlueConstants.AWS_ACCESS_KEY_ID));
    assertEquals("dlf-ak", filtered.get(PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_ID));
  }

  @Test
  void testOmitCloudAccessKeyPairProperties() {
    Map<String, String> secrets =
        Map.of(
            S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
            "AKIATEST",
            S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
            "secret",
            CloudStorageCredentialPropertyKeys.JDBC_USER,
            "db-user",
            CloudStorageCredentialPropertyKeys.JDBC_PASSWORD,
            "db-pass",
            "custom-token",
            "tok");

    Map<String, String> filtered =
        CloudStorageCredentialPropertyKeys.omitCloudAccessKeyPairProperties(secrets);

    assertEquals("tok", filtered.get("custom-token"));
    assertFalse(filtered.containsKey(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertFalse(filtered.containsKey(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
    assertFalse(filtered.containsKey(CloudStorageCredentialPropertyKeys.JDBC_USER));
    assertFalse(filtered.containsKey(CloudStorageCredentialPropertyKeys.JDBC_PASSWORD));
  }

  @Test
  void testCloudAccessKeyPairKeyDetection() {
    assertTrue(
        CloudStorageCredentialPropertyKeys.isCloudAccessKeyPairKey(
            S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertTrue(
        CloudStorageCredentialPropertyKeys.isCloudAccessKeyPairKey(
            AWSProperties.GRAVITINO_AWS_SECRET_ACCESS_KEY));
    assertTrue(
        CloudStorageCredentialPropertyKeys.isCloudAccessKeyPairKey(
            PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_SECRET));
    assertTrue(
        CloudStorageCredentialPropertyKeys.isCloudAccessKeyPairKey(
            PaimonConstants.GRAVITINO_DLF_SECURITY_TOKEN));
    assertTrue(
        CloudStorageCredentialPropertyKeys.isCloudAccessKeyPairKey(
            CloudStorageCredentialPropertyKeys.JDBC_USER));
    assertTrue(
        CloudStorageCredentialPropertyKeys.isCloudAccessKeyPairKey(
            CloudStorageCredentialPropertyKeys.JDBC_PASSWORD));
    assertFalse(CloudStorageCredentialPropertyKeys.isCloudAccessKeyPairKey("custom-token"));
  }

  @Test
  void testOmitCloudAccessKeyPairOmitsDlfSecurityToken() {
    Map<String, String> secrets =
        Map.of(
            PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_ID,
            "dlf-ak",
            PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_SECRET,
            "dlf-sk",
            PaimonConstants.GRAVITINO_DLF_SECURITY_TOKEN,
            "dlf-token",
            CloudStorageCredentialPropertyKeys.JDBC_PASSWORD,
            "db-pass",
            "other-secret",
            "keep");

    Map<String, String> filtered =
        CloudStorageCredentialPropertyKeys.omitCloudAccessKeyPairProperties(secrets);

    assertEquals("keep", filtered.get("other-secret"));
    assertFalse(filtered.containsKey(PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_ID));
    assertFalse(filtered.containsKey(PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_SECRET));
    assertFalse(filtered.containsKey(PaimonConstants.GRAVITINO_DLF_SECURITY_TOKEN));
    assertFalse(filtered.containsKey(CloudStorageCredentialPropertyKeys.JDBC_PASSWORD));
  }

  @Test
  void testOmitCloudAccessKeyPairPropertiesWithScopedKeys() {
    Map<String, String> secrets =
        Map.of(
            CloudStorageCredentialPropertyKeys.JDBC_USER,
            "db-user",
            CloudStorageCredentialPropertyKeys.JDBC_PASSWORD,
            "db-pass",
            S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
            "AKIATEST",
            "custom-secret",
            "keep");

    Map<String, String> jdbcOnly =
        CloudStorageCredentialPropertyKeys.omitCloudAccessKeyPairProperties(
            secrets,
            Set.of(
                CloudStorageCredentialPropertyKeys.JDBC_USER,
                CloudStorageCredentialPropertyKeys.JDBC_PASSWORD));

    assertEquals("keep", jdbcOnly.get("custom-secret"));
    assertEquals("AKIATEST", jdbcOnly.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertFalse(jdbcOnly.containsKey(CloudStorageCredentialPropertyKeys.JDBC_USER));
    assertFalse(jdbcOnly.containsKey(CloudStorageCredentialPropertyKeys.JDBC_PASSWORD));

    Map<String, String> emptyScope =
        CloudStorageCredentialPropertyKeys.omitCloudAccessKeyPairProperties(secrets, Set.of());
    assertEquals("db-user", emptyScope.get(CloudStorageCredentialPropertyKeys.JDBC_USER));
    assertEquals("AKIATEST", emptyScope.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
  }

  @Test
  void testKeysForCredentialType() {
    assertEquals(
        Set.of(
            S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY),
        CloudStorageCredentialPropertyKeys.keysForCredentialType(
            S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE));
    assertEquals(
        Set.of(
            CloudStorageCredentialPropertyKeys.JDBC_USER,
            CloudStorageCredentialPropertyKeys.JDBC_PASSWORD),
        CloudStorageCredentialPropertyKeys.keysForCredentialType(
            JdbcCredential.JDBC_CREDENTIAL_TYPE));
    assertTrue(
        CloudStorageCredentialPropertyKeys.keysForCredentialType(
                S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE)
            .isEmpty());
    assertTrue(
        CloudStorageCredentialPropertyKeys.keysForCredentialProviders(
                S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE
                    + ","
                    + JdbcCredential.JDBC_CREDENTIAL_TYPE)
            .contains(CloudStorageCredentialPropertyKeys.JDBC_PASSWORD));
    assertFalse(
        CloudStorageCredentialPropertyKeys.keysForCredentialProviders(
                S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE)
            .contains(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
  }
}
