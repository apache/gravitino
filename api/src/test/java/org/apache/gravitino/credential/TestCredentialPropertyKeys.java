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
package org.apache.gravitino.credential;

import java.util.HashSet;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestCredentialPropertyKeys {

  /**
   * Keys that appear in {@link Credential#credentialInfo()} but are never catalog entity
   * properties. Must stay disjoint from {@link CredentialPropertyKeys}.
   */
  private static final Set<String> VENDED_ONLY_KEYS =
      Set.of(
          S3TokenCredential.GRAVITINO_S3_TOKEN,
          OSSTokenCredential.GRAVITINO_OSS_TOKEN,
          COSTokenCredential.GRAVITINO_COS_SESSION_TOKEN,
          ADLSTokenCredential.GRAVITINO_ADLS_SAS_TOKEN,
          GCSTokenCredential.GCS_TOKEN_NAME,
          AwsIrsaCredential.ACCESS_KEY_ID,
          AwsIrsaCredential.SECRET_ACCESS_KEY,
          AwsIrsaCredential.SESSION_TOKEN);

  @Test
  void testCatalogPropertyCredentialKeys() {
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("s3-access-key-id"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("s3-secret-access-key"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("oss-access-key-id"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("oss-secret-access-key"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("cos-access-key-id"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("cos-secret-access-key"));
    Assertions.assertTrue(
        CredentialPropertyKeys.isCredentialPropertyKey("azure-storage-account-name"));
    Assertions.assertTrue(
        CredentialPropertyKeys.isCredentialPropertyKey("azure-storage-account-key"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("jdbc-user"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("jdbc-password"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("aws-access-key-id"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("aws-secret-access-key"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("dlf-access-key-id"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("dlf-access-key-secret"));
    Assertions.assertTrue(CredentialPropertyKeys.isCredentialPropertyKey("dlf-security-token"));
  }

  @Test
  void testVendedOnlyAndNonCredentialKeys() {
    Assertions.assertFalse(CredentialPropertyKeys.isCredentialPropertyKey(null));
    // Vended-only — not PropertiesMetadata / catalog entity properties.
    Assertions.assertFalse(CredentialPropertyKeys.isCredentialPropertyKey("s3-session-token"));
    Assertions.assertFalse(CredentialPropertyKeys.isCredentialPropertyKey("oss-security-token"));
    Assertions.assertFalse(CredentialPropertyKeys.isCredentialPropertyKey("cos-security-token"));
    Assertions.assertFalse(CredentialPropertyKeys.isCredentialPropertyKey("adls-sas-token"));
    Assertions.assertFalse(CredentialPropertyKeys.isCredentialPropertyKey("token"));
    Assertions.assertFalse(CredentialPropertyKeys.isCredentialPropertyKey("access-key-id"));
    Assertions.assertFalse(CredentialPropertyKeys.isCredentialPropertyKey("secret-access-key"));
    Assertions.assertFalse(CredentialPropertyKeys.isCredentialPropertyKey("session-token"));
    Assertions.assertFalse(CredentialPropertyKeys.isCredentialPropertyKey("custom-token"));
    Assertions.assertFalse(CredentialPropertyKeys.isCredentialPropertyKey("credential-providers"));
  }

  @Test
  void testSpiCredentialInfoKeysAreCatalogOrVendedOnly() {
    Set<String> catalogKeys = CredentialPropertyKeys.keys();
    for (String vendedOnly : VENDED_ONLY_KEYS) {
      Assertions.assertFalse(
          catalogKeys.contains(vendedOnly),
          () -> "vended-only key must not be in CredentialPropertyKeys: " + vendedOnly);
    }

    Set<String> seenTypes = new HashSet<>();
    ServiceLoader<Credential> loader = ServiceLoader.load(Credential.class);
    int loaded = 0;
    for (Credential uninitialized : loader) {
      loaded++;
      String type = uninitialized.credentialType();
      Assertions.assertTrue(seenTypes.add(type), () -> "duplicate SPI credential type: " + type);
      Credential sample = sampleCredential(uninitialized);
      Assertions.assertFalse(
          sample.credentialInfo().isEmpty(), () -> type + " credentialInfo() is empty");
      for (String key : sample.credentialInfo().keySet()) {
        Assertions.assertTrue(
            catalogKeys.contains(key) || VENDED_ONLY_KEYS.contains(key),
            () ->
                type
                    + " credentialInfo key \""
                    + key
                    + "\" is neither a CredentialPropertyKeys catalog key nor on the vended-only"
                    + " allowlist; add it to one of those sets so getSecrets cannot leak it");
      }
    }
    Assertions.assertTrue(loaded > 0, "Credential SPI loaded no implementations");
  }

  /**
   * Initializes a SPI-loaded {@link Credential} so {@link Credential#credentialInfo()} is
   * populated. Token types use a non-zero expire time; static types use 0.
   */
  private static Credential sampleCredential(Credential uninitialized) {
    String type = uninitialized.credentialType();
    switch (type) {
      case S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE:
        uninitialized.initialize(
            Map.of(
                S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID,
                "ak",
                S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY,
                "sk"),
            0);
        return uninitialized;
      case S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE:
        uninitialized.initialize(
            Map.of(
                S3TokenCredential.GRAVITINO_S3_SESSION_ACCESS_KEY_ID,
                "ak",
                S3TokenCredential.GRAVITINO_S3_SESSION_SECRET_ACCESS_KEY,
                "sk",
                S3TokenCredential.GRAVITINO_S3_TOKEN,
                "tok"),
            1);
        return uninitialized;
      case OSSSecretKeyCredential.OSS_SECRET_KEY_CREDENTIAL_TYPE:
        uninitialized.initialize(
            Map.of(
                OSSSecretKeyCredential.GRAVITINO_OSS_STATIC_ACCESS_KEY_ID,
                "ak",
                OSSSecretKeyCredential.GRAVITINO_OSS_STATIC_SECRET_ACCESS_KEY,
                "sk"),
            0);
        return uninitialized;
      case OSSTokenCredential.OSS_TOKEN_CREDENTIAL_TYPE:
        uninitialized.initialize(
            Map.of(
                OSSTokenCredential.GRAVITINO_OSS_SESSION_ACCESS_KEY_ID,
                "ak",
                OSSTokenCredential.GRAVITINO_OSS_SESSION_SECRET_ACCESS_KEY,
                "sk",
                OSSTokenCredential.GRAVITINO_OSS_TOKEN,
                "tok"),
            1);
        return uninitialized;
      case COSSecretKeyCredential.COS_SECRET_KEY_CREDENTIAL_TYPE:
        uninitialized.initialize(
            Map.of(
                COSSecretKeyCredential.GRAVITINO_COS_STATIC_ACCESS_KEY_ID,
                "ak",
                COSSecretKeyCredential.GRAVITINO_COS_STATIC_SECRET_ACCESS_KEY,
                "sk"),
            0);
        return uninitialized;
      case COSTokenCredential.COS_TOKEN_CREDENTIAL_TYPE:
        uninitialized.initialize(
            Map.of(
                COSTokenCredential.GRAVITINO_COS_SESSION_ACCESS_KEY_ID,
                "ak",
                COSTokenCredential.GRAVITINO_COS_SESSION_SECRET_ACCESS_KEY,
                "sk",
                COSTokenCredential.GRAVITINO_COS_SESSION_TOKEN,
                "tok"),
            1);
        return uninitialized;
      case AzureAccountKeyCredential.AZURE_ACCOUNT_KEY_CREDENTIAL_TYPE:
        uninitialized.initialize(
            Map.of(
                AzureAccountKeyCredential.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME,
                "acct",
                AzureAccountKeyCredential.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY,
                "key"),
            0);
        return uninitialized;
      case ADLSTokenCredential.ADLS_TOKEN_CREDENTIAL_TYPE:
        uninitialized.initialize(
            Map.of(
                ADLSTokenCredential.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME,
                "acct",
                ADLSTokenCredential.GRAVITINO_ADLS_SAS_TOKEN,
                "sas"),
            1);
        return uninitialized;
      case GCSTokenCredential.GCS_TOKEN_CREDENTIAL_TYPE:
        uninitialized.initialize(Map.of(GCSTokenCredential.GCS_TOKEN_NAME, "tok"), 1);
        return uninitialized;
      case AwsIrsaCredential.AWS_IRSA_CREDENTIAL_TYPE:
        uninitialized.initialize(
            Map.of(
                AwsIrsaCredential.ACCESS_KEY_ID,
                "ak",
                AwsIrsaCredential.SECRET_ACCESS_KEY,
                "sk",
                AwsIrsaCredential.SESSION_TOKEN,
                "tok"),
            1);
        return uninitialized;
      case JdbcCredential.JDBC_CREDENTIAL_TYPE:
        uninitialized.initialize(
            Map.of(
                JdbcCredential.GRAVITINO_JDBC_USER,
                "u",
                JdbcCredential.GRAVITINO_JDBC_PASSWORD,
                "p"),
            0);
        return uninitialized;
      case AwsSecretKeyCredential.AWS_SECRET_KEY_CREDENTIAL_TYPE:
        uninitialized.initialize(
            Map.of(
                AwsSecretKeyCredential.GRAVITINO_AWS_ACCESS_KEY_ID,
                "ak",
                AwsSecretKeyCredential.GRAVITINO_AWS_SECRET_ACCESS_KEY,
                "sk"),
            0);
        return uninitialized;
      case DlfSecretKeyCredential.DLF_SECRET_KEY_CREDENTIAL_TYPE:
        uninitialized.initialize(
            Map.of(
                DlfSecretKeyCredential.GRAVITINO_DLF_ACCESS_KEY_ID,
                "ak",
                DlfSecretKeyCredential.GRAVITINO_DLF_ACCESS_KEY_SECRET,
                "sk",
                DlfSecretKeyCredential.GRAVITINO_DLF_SECURITY_TOKEN,
                "tok"),
            0);
        return uninitialized;
      default:
        throw new AssertionError(
            "Credential SPI type \""
                + type
                + "\" ("
                + uninitialized.getClass().getName()
                + ") has no sample in TestCredentialPropertyKeys; add initialize() coverage so"
                + " credentialInfo() keys stay classified");
    }
  }
}
