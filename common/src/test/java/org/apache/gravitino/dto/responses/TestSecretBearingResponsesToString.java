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
package org.apache.gravitino.dto.responses;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSecretBearingResponsesToString {

  @Test
  public void testOAuth2TokenResponseDoesNotLeakTokens() {
    OAuth2TokenResponse response =
        new OAuth2TokenResponse(
            "secret-access-token-123",
            "access_token",
            "bearer",
            3600,
            "read write",
            "secret-refresh-token-456");

    // Before the fix, Lombok @ToString rendered the bearer credentials in plaintext, so logging
    // the response leaked them.
    Assertions.assertFalse(response.toString().contains("secret-access-token-123"));
    Assertions.assertFalse(response.toString().contains("secret-refresh-token-456"));
    // Non-secret fields stay visible so the string remains useful for debugging.
    Assertions.assertTrue(response.toString().contains("3600"));
    Assertions.assertTrue(response.toString().contains("read write"));
  }

  @Test
  public void testSecretsResponseDoesNotLeakSecretValues() {
    SecretsResponse response =
        new SecretsResponse(ImmutableMap.of("kms.key", "super-secret-material"));

    Assertions.assertFalse(response.toString().contains("super-secret-material"));
  }
}
