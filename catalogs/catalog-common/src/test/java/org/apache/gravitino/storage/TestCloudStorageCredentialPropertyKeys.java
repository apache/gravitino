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
import org.apache.gravitino.catalog.glue.GlueConstants;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;
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
    // Access key ID is non-hidden plaintext in properties(); GVFS keeps it.
    assertEquals("AKIATEST", filtered.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertFalse(filtered.containsKey(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
    assertFalse(filtered.containsKey("masked-any-key"));
  }

  @Test
  void testStaticCredentialKeyDetection() {
    assertTrue(
        CloudStorageCredentialPropertyKeys.isStaticCredentialKey(
            COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET));
    assertFalse(
        CloudStorageCredentialPropertyKeys.isStaticCredentialKey(
            COSProperties.GRAVITINO_COS_ACCESS_KEY_ID));
    assertFalse(
        CloudStorageCredentialPropertyKeys.isStaticCredentialKey(
            COSProperties.GRAVITINO_COS_REGION));

    // Secret-bearing keys from Glue/Paimon-DLF are static credentials.
    assertTrue(
        CloudStorageCredentialPropertyKeys.isStaticCredentialKey(
            AzureProperties.GRAVITINO_AZURE_CLIENT_SECRET));
    assertTrue(
        CloudStorageCredentialPropertyKeys.isStaticCredentialKey(
            GlueConstants.AWS_SECRET_ACCESS_KEY));
    assertTrue(
        CloudStorageCredentialPropertyKeys.isStaticCredentialKey(
            PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_SECRET));
    assertTrue(
        CloudStorageCredentialPropertyKeys.isStaticCredentialKey(
            PaimonConstants.GRAVITINO_DLF_SECURITY_TOKEN));

    // Access key IDs are non-hidden identifiers, not secrets; they behave like s3/oss/cos IDs.
    assertFalse(
        CloudStorageCredentialPropertyKeys.isStaticCredentialKey(GlueConstants.AWS_ACCESS_KEY_ID));
    assertFalse(
        CloudStorageCredentialPropertyKeys.isStaticCredentialKey(
            PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_ID));
  }

  @Test
  void testModuleSecretKeysStrippedButAccessKeyIdsSurvive() {
    Map<String, String> input =
        Map.of(
            AzureProperties.GRAVITINO_AZURE_CLIENT_SECRET, "aad-secret",
            GlueConstants.AWS_ACCESS_KEY_ID, "ak",
            GlueConstants.AWS_SECRET_ACCESS_KEY, "sk",
            PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_ID, "dlf-ak",
            PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_SECRET, "dlf-sk",
            PaimonConstants.GRAVITINO_DLF_SECURITY_TOKEN, "dlf-token");

    Map<String, String> filtered =
        CloudStorageCredentialPropertyKeys.omitStaticCredentialProperties(input);

    // The four secret-bearing keys must be stripped.
    assertFalse(filtered.containsKey(AzureProperties.GRAVITINO_AZURE_CLIENT_SECRET));
    assertFalse(filtered.containsKey(GlueConstants.AWS_SECRET_ACCESS_KEY));
    assertFalse(filtered.containsKey(PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_SECRET));
    assertFalse(filtered.containsKey(PaimonConstants.GRAVITINO_DLF_SECURITY_TOKEN));

    // Access key IDs are non-hidden identifiers and must survive in properties().
    assertEquals("ak", filtered.get(GlueConstants.AWS_ACCESS_KEY_ID));
    assertEquals("dlf-ak", filtered.get(PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_ID));
  }
}
