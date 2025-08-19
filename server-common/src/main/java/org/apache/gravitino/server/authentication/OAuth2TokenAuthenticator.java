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

import com.google.common.base.Preconditions;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.exceptions.UnauthorizedException;

/**
 * OAuth2TokenAuthenticator provides the OAuth 2.0 authentication support. OAuth2TokenAuthenticator
 * supports pluggable token validators for different validation mechanisms.
 */
class OAuth2TokenAuthenticator implements Authenticator {
  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(OAuth2TokenAuthenticator.class);

  private String serviceAudience;
  private OAuthTokenValidator tokenValidator;

  @Override
  public boolean isDataFromToken() {
    return true;
  }

  @Override
  public void initialize(Config config) throws RuntimeException {
    this.serviceAudience = config.get(OAuthConfig.SERVICE_AUDIENCE);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(serviceAudience),
        "Service audience cannot be blank for OAuth authentication");

    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.get(OAuthConfig.DEFAULT_TOKEN_PATH)),
        "The path for token of the default OAuth server can't be blank");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.get(OAuthConfig.DEFAULT_SERVER_URI)),
        "The uri of the default OAuth server can't be blank");

    // Create the appropriate token validator based on configuration
    this.tokenValidator = OAuthTokenValidatorFactory.createValidator(config);
  }

  @Override
  public Principal authenticateToken(byte[] tokenData) {
    if (tokenData == null) {
      LOG.warn("Empty token authorization header");
      throw new UnauthorizedException("Empty token authorization header");
    }
    String authData = new String(tokenData, StandardCharsets.UTF_8);
    if (StringUtils.isBlank(authData)
        || !authData.startsWith(AuthConstants.AUTHORIZATION_BEARER_HEADER)) {
      LOG.warn("Invalid token authorization header format");
      throw new UnauthorizedException("Invalid token authorization header");
    }
    String token = authData.substring(AuthConstants.AUTHORIZATION_BEARER_HEADER.length());
    if (StringUtils.isBlank(token)) {
      throw new UnauthorizedException("Blank token found");
    }
    // TODO: If we support multiple OAuth 2.0 servers, we should use multiple
    // signing keys.
    try {
      return tokenValidator.validateToken(token, serviceAudience);
    } catch (UnauthorizedException e) {
      // Re-throw validation errors (audience, subject, etc.) without wrapping
      throw e;
    } catch (Exception e) {
      LOG.error("JWT parse error: {}", e.getMessage(), e);
      throw new UnauthorizedException(e, "JWT parse error");
    }
  }

  @Override
  public boolean supportsToken(byte[] tokenData) {
    return tokenData != null
        && new String(tokenData, StandardCharsets.UTF_8)
            .startsWith(AuthConstants.AUTHORIZATION_BEARER_HEADER);
  }
}
