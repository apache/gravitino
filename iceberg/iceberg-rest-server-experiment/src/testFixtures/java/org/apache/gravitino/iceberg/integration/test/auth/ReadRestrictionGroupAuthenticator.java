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
package org.apache.gravitino.iceberg.integration.test.auth;

import static org.apache.gravitino.utils.PrincipalUtils.ANONYMOUS_PRINCIPAL;

import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.UserGroup;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.server.authentication.Authenticator;

/** Basic authentication fixture that gives the normal test user a known group membership. */
public class ReadRestrictionGroupAuthenticator implements Authenticator {

  /** Group used by the read restriction integration tests. */
  public static final String AUDITORS_GROUP = "read_restriction_auditors";

  /** {@inheritDoc} */
  @Override
  public boolean isDataFromToken() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public Principal authenticateToken(@Nullable byte[] tokenData) {
    if (tokenData == null || !supportsToken(tokenData)) {
      return ANONYMOUS_PRINCIPAL;
    }
    String authData = new String(tokenData, StandardCharsets.UTF_8);
    try {
      String credential = authData.substring(AuthConstants.AUTHORIZATION_BASIC_HEADER.length());
      String userName =
          new String(Base64.getDecoder().decode(credential), StandardCharsets.UTF_8)
              .split(":", -1)[0];
      if (userName.isEmpty()) {
        return ANONYMOUS_PRINCIPAL;
      }
      List<UserGroup> groups =
          "normal".equals(userName)
              ? Collections.singletonList(new UserGroup(Optional.empty(), AUDITORS_GROUP))
              : Collections.emptyList();
      return new UserPrincipal(userName, groups, authData);
    } catch (IllegalArgumentException e) {
      return ANONYMOUS_PRINCIPAL;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void initialize(Config config) {
    // The fixture does not need configuration.
  }

  /** {@inheritDoc} */
  @Override
  public boolean supportsToken(@Nullable byte[] tokenData) {
    return tokenData == null
        || new String(tokenData, StandardCharsets.UTF_8)
            .startsWith(AuthConstants.AUTHORIZATION_BASIC_HEADER);
  }
}
