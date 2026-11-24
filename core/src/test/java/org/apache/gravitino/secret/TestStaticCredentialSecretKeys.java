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
package org.apache.gravitino.secret;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Set;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.storage.AWSProperties;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.Test;

public class TestStaticCredentialSecretKeys {

  @Test
  void testKeysForCredentialType() {
    assertEquals(
        Set.of(
            S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY),
        StaticCredentialSecretKeys.keysForCredentialType(
            S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE));
    assertEquals(
        Set.of(StaticCredentialSecretKeys.JDBC_USER, StaticCredentialSecretKeys.JDBC_PASSWORD),
        StaticCredentialSecretKeys.keysForCredentialType(JdbcCredential.JDBC_CREDENTIAL_TYPE));
    assertTrue(
        StaticCredentialSecretKeys.keysForCredentialType(S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE)
            .isEmpty());
  }

  @Test
  void testKeysForCredentialProviders() {
    assertTrue(
        StaticCredentialSecretKeys.keysForCredentialProviders(
                S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE
                    + ","
                    + JdbcCredential.JDBC_CREDENTIAL_TYPE)
            .contains(StaticCredentialSecretKeys.JDBC_PASSWORD));
    assertFalse(
        StaticCredentialSecretKeys.keysForCredentialProviders(
                S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE)
            .contains(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
  }

  @Test
  void testOmitStaticCredentialSecrets() {
    Map<String, String> secrets =
        Map.of(
            S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
            "AKIATEST",
            S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
            "secret",
            StaticCredentialSecretKeys.JDBC_USER,
            "db-user",
            StaticCredentialSecretKeys.JDBC_PASSWORD,
            "db-pass",
            "custom-token",
            "tok");

    Map<String, String> filtered = StaticCredentialSecretKeys.omitStaticCredentialSecrets(secrets);

    assertEquals("tok", filtered.get("custom-token"));
    assertFalse(filtered.containsKey(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertFalse(filtered.containsKey(StaticCredentialSecretKeys.JDBC_USER));
  }

  @Test
  void testIsStaticCredentialSecretKey() {
    assertTrue(
        StaticCredentialSecretKeys.isStaticCredentialSecretKey(
            AWSProperties.GRAVITINO_AWS_SECRET_ACCESS_KEY));
    assertTrue(
        StaticCredentialSecretKeys.isStaticCredentialSecretKey(
            PaimonConstants.GRAVITINO_DLF_SECURITY_TOKEN));
    assertFalse(StaticCredentialSecretKeys.isStaticCredentialSecretKey("custom-token"));
  }

  @Test
  void testOmitStaticCredentialSecretsWithScopedKeys() {
    Map<String, String> secrets =
        Map.of(
            StaticCredentialSecretKeys.JDBC_USER,
            "db-user",
            S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
            "AKIATEST",
            "custom-secret",
            "keep");

    Map<String, String> jdbcOnly =
        StaticCredentialSecretKeys.omitStaticCredentialSecrets(
            secrets, Set.of(StaticCredentialSecretKeys.JDBC_USER));

    assertEquals("keep", jdbcOnly.get("custom-secret"));
    assertEquals("AKIATEST", jdbcOnly.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertFalse(jdbcOnly.containsKey(StaticCredentialSecretKeys.JDBC_USER));
  }
}
