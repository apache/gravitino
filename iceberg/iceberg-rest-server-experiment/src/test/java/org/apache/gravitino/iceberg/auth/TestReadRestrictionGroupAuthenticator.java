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
package org.apache.gravitino.iceberg.auth;

import static org.apache.gravitino.utils.PrincipalUtils.ANONYMOUS_PRINCIPAL;

import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Base64;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.iceberg.integration.test.auth.ReadRestrictionGroupAuthenticator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestReadRestrictionGroupAuthenticator {

  private final ReadRestrictionGroupAuthenticator authenticator =
      new ReadRestrictionGroupAuthenticator();

  @Test
  void testNormalUserReceivesAuditorsGroupAndOriginalToken() {
    String authorization = basic("normal:mock");
    UserPrincipal principal =
        (UserPrincipal)
            authenticator.authenticateToken(authorization.getBytes(StandardCharsets.UTF_8));
    Assertions.assertEquals("normal", principal.getName());
    Assertions.assertEquals(1, principal.getGroups().size());
    Assertions.assertEquals(
        ReadRestrictionGroupAuthenticator.AUDITORS_GROUP,
        principal.getGroups().get(0).getGroupName());
    Assertions.assertEquals(authorization, principal.getAccessToken().orElseThrow());
  }

  @ParameterizedTest
  @ValueSource(strings = {"super:mock", "other:mock", "other", "other:password:with:colons"})
  void testOtherUsersHaveNoGroups(String credential) {
    String authorization = basic(credential);
    UserPrincipal principal =
        (UserPrincipal)
            authenticator.authenticateToken(authorization.getBytes(StandardCharsets.UTF_8));
    Assertions.assertEquals(credential.split(":")[0], principal.getName());
    Assertions.assertTrue(principal.getGroups().isEmpty());
    Assertions.assertEquals(authorization, principal.getAccessToken().orElseThrow());
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(
      strings = {"", " ", "Bearer token", "Basic ", "Basic !!!", "Basic Og==", "Basic Om1vY2s="})
  void testInvalidOrMissingCredentialsAreAnonymous(String authorization) {
    byte[] token = authorization == null ? null : authorization.getBytes(StandardCharsets.UTF_8);
    Principal principal = authenticator.authenticateToken(token);
    Assertions.assertSame(ANONYMOUS_PRINCIPAL, principal);
  }

  @Test
  void testSupportedTokens() {
    Assertions.assertTrue(authenticator.isDataFromToken());
    Assertions.assertTrue(authenticator.supportsToken(null));
    Assertions.assertTrue(
        authenticator.supportsToken(basic("normal:mock").getBytes(StandardCharsets.UTF_8)));
    Assertions.assertFalse(
        authenticator.supportsToken("Bearer token".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertFalse(authenticator.supportsToken(new byte[0]));
  }

  private static String basic(String credential) {
    return "Basic "
        + Base64.getEncoder().encodeToString(credential.getBytes(StandardCharsets.UTF_8));
  }
}
