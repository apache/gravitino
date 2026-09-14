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
package org.apache.gravitino.lance.common.ops.gravitino;

import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.ActiveRoles;
import org.apache.gravitino.auth.ActiveRolesParser;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.client.CustomTokenProvider;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.lance.common.config.LanceConfig;
import org.apache.gravitino.utils.PrincipalUtils;

/** Reads the authenticated caller on each request without retaining credentials in the client. */
final class LanceCallerTokenProvider extends CustomTokenProvider {

  private static final String RESTORE_SERVICE_ACCOUNT_HINT =
      "Set "
          + LanceConfig.LANCE_CONFIG_PREFIX
          + "gravitino-"
          + LanceConfig.CONFIG_AUTH_TYPE
          + "=simple (or oauth2) with its service credentials to use a service account instead.";

  @Override
  public byte[] getTokenData() {
    return getCustomTokenInfo().getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public Map<String, String> getRequestHeaders() {
    ActiveRoles roles = caller().getActiveRoles();
    String value =
        roles.isAll()
            ? ActiveRolesParser.ALL_KEYWORD
            : roles.isNone() ? ActiveRolesParser.NONE_KEYWORD : String.join(",", roles.roleNames());
    return Collections.singletonMap(AuthConstants.X_GRAVITINO_ACTIVE_ROLES_HEADER, value);
  }

  @Override
  protected String getCustomTokenInfo() {
    String token =
        caller().getAccessToken().orElseThrow(LanceCallerTokenProvider::missingCredentials);
    // Kerberos negotiation tokens cannot be replayed to a different service. Forward only
    // credentials that the remote server can independently validate without a token exchange.
    if (!(token.startsWith(AuthConstants.AUTHORIZATION_BASIC_HEADER)
            || token.startsWith(AuthConstants.AUTHORIZATION_BEARER_HEADER))
        || token.substring(token.indexOf(' ') + 1).trim().isEmpty()) {
      throw new UnauthorizedException(
          "Standalone Lance REST caller authentication requires Basic or Bearer credentials. "
              + RESTORE_SERVICE_ACCOUNT_HINT);
    }
    return token;
  }

  private static UserPrincipal caller() {
    Principal principal = PrincipalUtils.getCurrentPrincipal();
    if (!(principal instanceof UserPrincipal)
        || AuthConstants.ANONYMOUS_USER.equals(principal.getName())) {
      throw missingCredentials();
    }
    return (UserPrincipal) principal;
  }

  private static UnauthorizedException missingCredentials() {
    return new UnauthorizedException(
        "Standalone Lance REST caller authentication requires authenticated caller credentials. "
            + RESTORE_SERVICE_ACCOUNT_HINT);
  }
}
