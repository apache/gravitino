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
package org.apache.gravitino.flink.connector.utils;

import java.util.Map;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.RESTException;
import org.apache.gravitino.secret.SupportsSecrets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPropertyUtils {

  @Test
  void testPropertiesWithSecretsOverlaysPlaintext() {
    Map<String, String> merged =
        PropertyUtils.propertiesWithSecrets(
            Map.of("jdbc-password", "******", "visible", "ok"),
            () -> () -> Map.of("jdbc-password", "s3cr3t"));
    Assertions.assertEquals("s3cr3t", merged.get("jdbc-password"));
    Assertions.assertEquals("ok", merged.get("visible"));
  }

  @Test
  void testPropertiesWithSecretsSwallowsNotFoundFromOlderServer() {
    SupportsSecrets broken =
        () -> {
          throw new NotFoundException("secrets endpoint not found");
        };
    Map<String, String> merged =
        PropertyUtils.propertiesWithSecrets(Map.of("jdbc-password", "******"), () -> broken);
    Assertions.assertEquals("******", merged.get("jdbc-password"));
  }

  @Test
  void testPropertiesWithSecretsPropagatesRestException() {
    SupportsSecrets broken =
        () -> {
          throw new RESTException("connection failed");
        };
    Assertions.assertThrows(
        RESTException.class,
        () -> PropertyUtils.propertiesWithSecrets(Map.of("k", "v"), () -> broken));
  }

  @Test
  void testPropertiesWithSecretsSwallowsUnsupportedOperation() {
    Map<String, String> merged =
        PropertyUtils.propertiesWithSecrets(
            Map.of("k", "v"),
            () -> {
              throw new UnsupportedOperationException("no secrets");
            });
    Assertions.assertEquals("v", merged.get("k"));
  }

  @Test
  void testPropertiesWithSecretsNullProperties() {
    Map<String, String> merged =
        PropertyUtils.propertiesWithSecrets(null, () -> () -> Map.of("secret", "x"));
    Assertions.assertEquals("x", merged.get("secret"));
  }

  @Test
  void testPropertiesWithSecretsAndCredentialsSkipsExpiringCredentials() {
    Credential staticCred = new S3SecretKeyCredential("AKIATEST", "static-secret");
    Credential tokenCred =
        new S3TokenCredential("SESSION", "session-secret", "tok", 1_700_000_000_000L);
    Map<String, String> merged =
        PropertyUtils.propertiesWithSecretsAndCredentials(
            Map.of("s3-endpoint", "http://s3.example.com"),
            () -> () -> Map.of(),
            () -> () -> new Credential[] {tokenCred, staticCred});
    Assertions.assertEquals(
        "AKIATEST", merged.get(S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID));
    Assertions.assertEquals(
        "static-secret", merged.get(S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY));
    Assertions.assertFalse(merged.containsKey(S3TokenCredential.GRAVITINO_S3_TOKEN));
    Assertions.assertEquals("http://s3.example.com", merged.get("s3-endpoint"));
  }
}
