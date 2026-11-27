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
package org.apache.gravitino.paimon.common;

import java.util.Map;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Tests translation of Gravitino and storage aliases without losing native Paimon options. */
public class TestPaimonCatalogOptions {

  @ParameterizedTest
  @CsvSource({
    "catalog-backend,metastore",
    "jdbc-user,jdbc.user",
    "jdbc-password,jdbc.password",
    "s3-access-key-id,s3.access-key",
    "s3-secret-access-key,s3.secret-key",
    "s3-session-token,s3.session-token",
    "s3.session.token,s3.session-token",
    "s3a.session-token,s3.session-token",
    "s3a.session.token,s3.session-token",
    "s3-endpoint,s3.endpoint",
    "oss-access-key-id,fs.oss.accessKeyId",
    "oss-secret-access-key,fs.oss.accessKeySecret",
    "oss-security-token,fs.oss.securityToken",
    "oss-endpoint,fs.oss.endpoint",
    "fs.s3a.access-key,s3.access-key",
    "fs.s3a.secret-key,s3.secret-key",
    "fs.s3a.access.key,s3.access-key",
    "fs.s3a.secret.key,s3.secret-key",
    "fs.s3a.session.token,s3.session-token",
    "fs.s3a.session-token,s3.session-token",
    "fs.oss.securitytoken,fs.oss.securityToken",
    "fs.oss.SecurityToken,fs.oss.securityToken",
    "fs.s3a.endpoint,s3.endpoint",
    "fs.s3a.path.style.access,s3.path.style.access"
  })
  void testAliases(String alias, String nativeKey) {
    Map<String, String> properties =
        Map.of(alias, "value", "client." + alias, "client-value", "custom.option", "preserved");
    Options options = PaimonCatalogOptions.fromProperties(properties);
    Assertions.assertEquals("value", options.get(nativeKey));
    Assertions.assertNull(options.get(alias));
    Assertions.assertEquals("client-value", options.get("client." + nativeKey));
    Assertions.assertNull(options.get("client." + alias));
    Assertions.assertEquals("preserved", options.get("custom.option"));
    Assertions.assertEquals("value", properties.get(alias));
  }

  @Test
  void testNativeOptionsAndConsistentAliases() {
    Options options =
        PaimonCatalogOptions.fromProperties(
            Map.of("catalog-backend", "hive", "metastore", "hive", "lock.enabled", "true"));
    Assertions.assertEquals("hive", options.get("metastore"));
    Assertions.assertEquals("true", options.get("lock.enabled"));
  }

  @Test
  void testConflictingAliasesDoNotExposeValues() {
    IllegalArgumentException failure =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                PaimonCatalogOptions.fromProperties(
                    Map.of("jdbc-password", "secret1", "jdbc.password", "secret2")));
    Assertions.assertFalse(failure.getMessage().contains("secret1"));
    Assertions.assertFalse(failure.getMessage().contains("secret2"));
  }

  @Test
  void testConflictingSessionTokenAliasesAreRejected() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            PaimonCatalogOptions.fromProperties(
                Map.of("s3.session-token", "secret1", "s3.session.token", "secret2")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            PaimonCatalogOptions.fromProperties(
                Map.of("fs.oss.securityToken", "secret1", "fs.oss.SECURITYTOKEN", "secret2")));
  }

  @Test
  void testInvalidDriverIsRejected() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> PaimonCatalogOptions.fromProperties(Map.of("jdbc-driver", "missing.Driver")));
  }
}
