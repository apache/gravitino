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

import java.security.Principal;
import java.util.Collections;
import java.util.Optional;
import org.apache.gravitino.UserGroup;
import org.apache.gravitino.UserPrincipal;

/** Supplies a group-bearing principal for Iceberg read-restriction integration tests. */
public class ReadRestrictionGroupAuthenticator extends SimpleAuthenticator {

  /** Group assigned to the normal test user. */
  public static final String AUDITORS_GROUP = "read_restriction_auditors";

  /** Creates the test authenticator. */
  public ReadRestrictionGroupAuthenticator() {}

  /** {@inheritDoc} */
  @Override
  public Principal authenticateToken(byte[] tokenData) {
    Principal principal = super.authenticateToken(tokenData);
    if (!(principal instanceof UserPrincipal) || !"normal".equals(principal.getName())) {
      return principal;
    }
    UserPrincipal user = (UserPrincipal) principal;
    return new UserPrincipal(
        user.getName(),
        Collections.singletonList(new UserGroup(Optional.empty(), AUDITORS_GROUP)),
        user.getAccessToken().orElse(null));
  }
}
