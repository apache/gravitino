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
package org.apache.gravitino.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.Test;

public class TestBaseCatalogPropertiesMetadata {

  private final PropertiesMetadata metadata =
      new BaseCatalogPropertiesMetadata() {
        @Override
        protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
          return Collections.emptyMap();
        }
      };

  @Test
  void testCredentialPropertyEntriesAreDeclaredForAllCatalogs() {
    assertTrue(metadata.containsProperty(CredentialConstants.CREDENTIAL_PROVIDERS));
    assertTrue(metadata.containsProperty(CredentialConstants.S3_TOKEN_EXPIRE_IN_SECS));
    assertFalse(metadata.isHiddenProperty(CredentialConstants.CREDENTIAL_PROVIDERS));
    assertFalse(metadata.isHiddenProperty(CredentialConstants.S3_TOKEN_EXPIRE_IN_SECS));
  }

  @Test
  void testSharedCloudCredentialKeysAreDeclaredForAllCatalogs() {
    assertTrue(metadata.containsProperty(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertTrue(metadata.containsProperty(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
    assertFalse(metadata.isHiddenProperty(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertTrue(metadata.isHiddenProperty(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
  }

  @Test
  void testConnectorCredentialKeysAreDeclaredForAllCatalogs() {
    assertTrue(metadata.containsProperty("aws-access-key-id"));
    assertFalse(metadata.isHiddenProperty("aws-access-key-id"));
    assertTrue(metadata.isHiddenProperty("aws-secret-access-key"));
    assertFalse(metadata.containsProperty("jdbc-user"));
    assertFalse(metadata.containsProperty("jdbc-password"));
    assertFalse(metadata.containsProperty("token-provider"));
    assertFalse(metadata.containsProperty("token"));
    assertFalse(metadata.containsProperty("dlf-access-key-id"));
    assertFalse(metadata.containsProperty("dlf-access-key-secret"));
    assertFalse(metadata.containsProperty("dlf-security-token"));
    assertFalse(metadata.containsProperty("dlf-token-path"));
    assertFalse(metadata.containsProperty("dlf-token-loader"));
    assertTrue(metadata.containsProperty("gcs.oauth2.token"));
    assertTrue(metadata.isHiddenProperty("gcs.oauth2.token"));
    assertTrue(metadata.containsProperty("gcs.oauth2.token-expires-at"));
    assertFalse(metadata.isHiddenProperty("gcs.oauth2.token-expires-at"));
    assertTrue(metadata.containsProperty("gcs.oauth2.refresh-credentials-endpoint"));
    assertFalse(metadata.isHiddenProperty("gcs.oauth2.refresh-credentials-endpoint"));
    assertTrue(metadata.containsProperty("s3.session-token-expires-at-ms"));
    assertFalse(metadata.isHiddenProperty("s3.session-token-expires-at-ms"));
    assertTrue(metadata.containsProperty("s3.session-token"));
    assertTrue(metadata.isHiddenProperty("s3.session-token"));
    assertFalse(metadata.containsProperty("jdbc.password"));
    assertFalse(metadata.containsProperty("jdbc.user"));
    assertTrue(metadata.containsProperty("adls.sas-token.account.dfs.core.windows.net"));
    assertTrue(metadata.isHiddenProperty("adls.sas-token.account.dfs.core.windows.net"));
    assertTrue(
        metadata.containsProperty("adls.sas-token-expires-at-ms.account.dfs.core.windows.net"));
    assertFalse(
        metadata.isHiddenProperty("adls.sas-token-expires-at-ms.account.dfs.core.windows.net"));

    assertEquals(
        metadata.isHiddenProperty(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY),
        metadata.isHiddenProperty("s3.session-token"));
    assertEquals(
        metadata.isHiddenProperty(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY),
        metadata.isHiddenProperty("gcs.oauth2.token"));
    assertEquals(
        metadata.isHiddenProperty(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY),
        metadata.isHiddenProperty("adls.sas-token.account.dfs.core.windows.net"));
    assertEquals(
        metadata.isHiddenProperty(S3Properties.GRAVITINO_S3_STS_ENDPOINT),
        metadata.isHiddenProperty("client.refresh-credentials-endpoint"));
    assertEquals(
        metadata.isHiddenProperty(S3Properties.GRAVITINO_S3_STS_ENDPOINT),
        metadata.isHiddenProperty("gcs.oauth2.refresh-credentials-endpoint"));
    assertEquals(
        metadata.isHiddenProperty(CredentialConstants.S3_TOKEN_EXPIRE_IN_SECS),
        metadata.isHiddenProperty("s3.session-token-expires-at-ms"));
    assertEquals(
        metadata.isHiddenProperty(CredentialConstants.OSS_TOKEN_EXPIRE_IN_SECS),
        metadata.isHiddenProperty("client.security-token-expires-at-ms"));
    assertFalse(metadata.isHiddenProperty("gcs.oauth2.token-expires-at"));
    assertEquals(
        metadata.isHiddenProperty(CredentialConstants.ADLS_TOKEN_EXPIRE_IN_SECS),
        metadata.isHiddenProperty("adls.sas-token-expires-at-ms.account.dfs.core.windows.net"));
  }

  @Test
  void testRuntimeCopiedS3AccessKeyUsesSharedCloudMetadata() {
    PropertiesMetadata glueLikeMetadata =
        new BaseCatalogPropertiesMetadata() {
          @Override
          protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
            return ImmutableMap.of(
                "aws-access-key-id",
                PropertyEntry.stringOptionalPropertyEntry(
                    "aws-access-key-id", "AWS access key ID", false, null, false),
                "aws-secret-access-key",
                PropertyEntry.stringOptionalPropertyEntry(
                    "aws-secret-access-key", "AWS secret access key", false, null, true));
          }
        };

    Map<String, String> properties =
        ImmutableMap.of(
            "aws-access-key-id",
            "AKIAEXAMPLE",
            S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
            "AKIAEXAMPLE",
            S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
            "secret-value");
    Map<String, String> masked =
        HiddenPropertyMaskUtils.maskHiddenProperties(properties, glueLikeMetadata);

    assertEquals("AKIAEXAMPLE", masked.get("aws-access-key-id"));
    assertEquals("AKIAEXAMPLE", masked.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertEquals(
        HiddenPropertyMaskUtils.MASKED_VALUE,
        masked.get(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
  }
}
