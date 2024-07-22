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

package org.apache.gravitino.server.authentication;

import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;

/**
 * SimpleAuthenticator will provide a basic authentication mechanism. SimpleAuthenticator will use
 * the identifier provided by the user without any validation.
 */
class SimpleAuthenticator implements Authenticator {

  private final Principal ANONYMOUS_PRINCIPAL = new UserPrincipal(AuthConstants.ANONYMOUS_USER);

  @Override
  public boolean isDataFromToken() {
    return true;
  }

  @Override
  public Principal authenticateToken(byte[] tokenData) {
    if (tokenData == null) {
      return ANONYMOUS_PRINCIPAL;
    }
    String authData = new String(tokenData, StandardCharsets.UTF_8);
    if (StringUtils.isBlank(authData)) {
      return ANONYMOUS_PRINCIPAL;
    }
    if (!authData.startsWith(AuthConstants.AUTHORIZATION_BASIC_HEADER)) {
      return ANONYMOUS_PRINCIPAL;
    }
    String credential = authData.substring(AuthConstants.AUTHORIZATION_BASIC_HEADER.length());
    if (StringUtils.isBlank(credential)) {
      return ANONYMOUS_PRINCIPAL;
    }
    try {
      String[] userInformation =
          new String(Base64.getDecoder().decode(credential), StandardCharsets.UTF_8).split(":");
      if (userInformation.length != 2) {
        return ANONYMOUS_PRINCIPAL;
      }
      return new UserPrincipal(userInformation[0]);
    } catch (IllegalArgumentException ie) {
      return ANONYMOUS_PRINCIPAL;
    }
  }

  @Override
  public void initialize(Config config) throws RuntimeException {
    // no op
  }

  @Override
  public boolean supportsToken(byte[] tokenData) {
    return tokenData == null
        || new String(tokenData, StandardCharsets.UTF_8)
            .startsWith(AuthConstants.AUTHORIZATION_BASIC_HEADER);
  }
}
