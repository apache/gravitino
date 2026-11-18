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
 *
 */
package org.apache.gravitino.lance.common.ops.gravitino;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.lance.common.config.LanceConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoLanceClientAuth {

  private static final String URI = "http://localhost:8090";
  private static final String METALAKE = "test_metalake";

  private static LanceConfig configOf(Map<String, String> properties) {
    return new LanceConfig(properties);
  }

  @Test
  void testAuthTypeDefaultsToSimple() {
    LanceConfig config = configOf(ImmutableMap.of("gravitino-metalake", METALAKE));
    Assertions.assertEquals(AuthProperties.SIMPLE_AUTH_TYPE, config.getGravitinoAuthType());
    Assertions.assertEquals(
        LanceConfig.DEFAULT_SIMPLE_USERNAME, config.get(LanceConfig.GRAVITINO_SIMPLE_USERNAME));
  }

  @Test
  void testSimpleAuthUsesConfiguredUserName() {
    LanceConfig config =
        configOf(
            ImmutableMap.of(
                "gravitino-metalake", METALAKE,
                "gravitino-auth-type", "simple",
                "gravitino-simple.user-name", "svc_lance"));
    Assertions.assertEquals("svc_lance", config.get(LanceConfig.GRAVITINO_SIMPLE_USERNAME));
    Assertions.assertDoesNotThrow(
        () ->
            GravitinoLanceNamespaceWrapper.newClientBuilder(
                URI, METALAKE, ImmutableMap.of(), config));
  }

  @Test
  void testAuthTypeIsCaseInsensitive() {
    LanceConfig config =
        configOf(ImmutableMap.of("gravitino-metalake", METALAKE, "gravitino-auth-type", "SIMPLE"));
    Assertions.assertDoesNotThrow(
        () ->
            GravitinoLanceNamespaceWrapper.newClientBuilder(
                URI, METALAKE, ImmutableMap.of(), config));
  }

  @Test
  void testOAuth2RequiresServerUri() {
    LanceConfig config =
        configOf(
            ImmutableMap.of(
                "gravitino-metalake", METALAKE,
                "gravitino-auth-type", "oauth2",
                "gravitino-oauth2.credential", "client:secret",
                "gravitino-oauth2.token-path", "/oauth/token",
                "gravitino-oauth2.scope", "test"));
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                GravitinoLanceNamespaceWrapper.newClientBuilder(
                    URI, METALAKE, ImmutableMap.of(), config));
    Assertions.assertTrue(exception.getMessage().contains("gravitino-oauth2.server-uri"));
  }

  @Test
  void testOAuth2RequiresCredential() {
    LanceConfig config =
        configOf(
            ImmutableMap.of(
                "gravitino-metalake", METALAKE,
                "gravitino-auth-type", "oauth2",
                "gravitino-oauth2.server-uri", "http://localhost:8177",
                "gravitino-oauth2.token-path", "/oauth/token",
                "gravitino-oauth2.scope", "test"));
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                GravitinoLanceNamespaceWrapper.newClientBuilder(
                    URI, METALAKE, ImmutableMap.of(), config));
    Assertions.assertTrue(exception.getMessage().contains("gravitino-oauth2.credential"));
  }

  @Test
  void testUnsupportedAuthTypeIsRejected() {
    LanceConfig config =
        configOf(
            ImmutableMap.of("gravitino-metalake", METALAKE, "gravitino-auth-type", "kerberos"));
    UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                GravitinoLanceNamespaceWrapper.newClientBuilder(
                    URI, METALAKE, ImmutableMap.of(), config));
    Assertions.assertTrue(exception.getMessage().contains("kerberos"));
  }
}
