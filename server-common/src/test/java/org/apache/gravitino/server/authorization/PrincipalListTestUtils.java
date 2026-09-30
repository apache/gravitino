/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.authorization;

import java.util.Locale;
import java.util.stream.IntStream;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;

/** Shared inputs for principal-list authorization tests. */
final class PrincipalListTestUtils {
  private PrincipalListTestUtils() {}

  static NameIdentifier[] principalIdentifiers(Entity.EntityType type, int count) {
    return IntStream.range(0, count)
        .mapToObj(
            i ->
                NameIdentifier.of(
                    principalNamespace(type, "testMetalake"),
                    type.name().toLowerCase(Locale.ROOT) + i))
        .toArray(NameIdentifier[]::new);
  }

  static Namespace principalNamespace(Entity.EntityType type, String metalake) {
    return switch (type) {
      case USER -> AuthorizationUtils.ofUserNamespace(metalake);
      case GROUP -> AuthorizationUtils.ofGroupNamespace(metalake);
      case ROLE -> AuthorizationUtils.ofRoleNamespace(metalake);
      default -> throw new IllegalArgumentException("Not a principal type: " + type);
    };
  }

  static String principalListExpression(Entity.EntityType type) {
    return switch (type) {
      case USER -> AuthorizationExpressionConstants.LOAD_USER_AUTHORIZATION_EXPRESSION;
      case GROUP -> AuthorizationExpressionConstants.LOAD_GROUP_AUTHORIZATION_EXPRESSION;
      case ROLE -> AuthorizationExpressionConstants.LOAD_ROLE_AUTHORIZATION_EXPRESSION;
      default -> throw new IllegalArgumentException("Not a principal type: " + type);
    };
  }

  static Privilege.Name principalManagementPrivilege(Entity.EntityType type) {
    return switch (type) {
      case USER -> Privilege.Name.MANAGE_USERS;
      case GROUP -> Privilege.Name.MANAGE_GROUPS;
      case ROLE -> Privilege.Name.MANAGE_GRANTS;
      default -> throw new IllegalArgumentException("Not a principal type: " + type);
    };
  }
}
