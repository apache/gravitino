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
package org.apache.gravitino.authorization;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.meta.DomainEntity;

/** Shared authorization rules for a domain and its current parent in the metalake. */
public final class DomainAccess {
  private DomainAccess() {}

  /** Returns the privilege required to read the supplied domain snapshot. */
  public static Privilege.Name readPrivilege(DomainEntity domain) {
    return domain.status() == Domain.Status.DRAFT
        ? Privilege.Name.MANAGE_DOMAIN
        : Privilege.Name.USE_DOMAIN;
  }

  /**
   * Builds the complete authorization scope from a domain snapshot with its current parent.
   *
   * @param metalake The metalake name.
   * @param domain The domain snapshot, or null for a metalake-only check.
   * @return The metalake, parent and domain scopes.
   */
  public static List<MetadataObject> scopes(String metalake, @Nullable DomainEntity domain) {
    List<MetadataObject> scopes = new ArrayList<>();
    scopes.add(MetadataObjects.of(null, metalake, MetadataObject.Type.METALAKE));
    if (domain != null) {
      domain
          .parent()
          .ifPresent(
              name -> scopes.add(MetadataObjects.of(null, name, MetadataObject.Type.DOMAIN)));
      scopes.add(MetadataObjects.of(null, domain.name(), MetadataObject.Type.DOMAIN));
    }
    return scopes;
  }

  /**
   * Evaluates ownership and allow/deny policies over the whole scope. Management implies usage; a
   * usage deny still overrides that implied usage. Owners bypass privilege denies.
   *
   * @param authorizer The authorizer.
   * @param principal The caller.
   * @param metalake The metalake name.
   * @param scopes The current authorization scopes.
   * @param privilege The requested privilege.
   * @param context The request context.
   * @return Whether the caller has the requested privilege.
   */
  public static boolean allowed(
      GravitinoAuthorizer authorizer,
      Principal principal,
      String metalake,
      List<MetadataObject> scopes,
      Privilege.Name privilege,
      AuthorizationRequestContext context) {
    if (scopes.stream()
        .anyMatch(scope -> authorizer.isOwner(principal, metalake, scope, context))) {
      return true;
    }
    if (scopes.stream()
        .anyMatch(scope -> authorizer.deny(principal, metalake, scope, privilege, context))) {
      return false;
    }
    boolean granted =
        scopes.stream()
            .anyMatch(
                scope -> authorizer.authorize(principal, metalake, scope, privilege, context));
    return granted
        || (privilege == Privilege.Name.USE_DOMAIN
            && allowed(
                authorizer, principal, metalake, scopes, Privilege.Name.MANAGE_DOMAIN, context));
  }
}
