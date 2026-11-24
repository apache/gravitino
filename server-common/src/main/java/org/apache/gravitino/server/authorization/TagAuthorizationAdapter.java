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
package org.apache.gravitino.server.authorization;

import java.security.Principal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.meta.PolicyEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Combines one privilege's RBAC scope checks with tag-policy evidence for its original target. */
public final class TagAuthorizationAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(TagAuthorizationAdapter.class);
  private final GravitinoAuthorizer authorizer;

  /**
   * Creates a composition adapter shared by point and list expression evaluation.
   *
   * @param authorizer the RBAC authorizer and optional tag matcher
   */
  public TagAuthorizationAdapter(GravitinoAuthorizer authorizer) {
    this.authorizer = Objects.requireNonNull(authorizer, "authorizer");
  }

  /**
   * Evaluates a privilege, preserving RBAC deny precedence and the existing scope order.
   *
   * @param principal the caller
   * @param metalake the containing metalake
   * @param scopes the RBAC scopes, ending with the target object
   * @param privilege the requested privilege
   * @param context the context shared by this request's checks
   * @return whether RBAC or a matching tag allows the privilege without an applicable RBAC deny
   */
  public boolean authorize(
      Principal principal,
      String metalake,
      List<MetadataObject> scopes,
      Privilege.Name privilege,
      AuthorizationRequestContext context) {
    boolean rbacAllows = false;
    for (MetadataObject scope : scopes) {
      if (scope != null && authorizer.authorize(principal, metalake, scope, privilege, context)) {
        rbacAllows = true;
        break;
      }
    }
    boolean tagEnabled = authorizer.isTagAuthorizationEnabled();
    if (!rbacAllows && !tagEnabled) {
      return false;
    }
    MetadataObject denyScope = findDeny(principal, metalake, scopes, privilege, context);
    if (rbacAllows && denyScope == null) {
      return true;
    }
    if (!tagEnabled || (denyScope != null && !LOG.isDebugEnabled())) {
      return false;
    }
    MetadataObject target = scopes.get(scopes.size() - 1);
    if (target == null) {
      return false;
    }
    if (denyScope != null) {
      logSuppressedTagAllow(principal, metalake, target, privilege, context, denyScope);
      return false;
    }
    Optional<PolicyEntity> match =
        authorizer.matchTagPolicy(principal, metalake, target, privilege, context);
    if (match.isEmpty()) {
      return false;
    }
    PolicyEntity policy = match.get();
    LOG.debug(
        "Tag allow: principal {}, metalake {}, object {}:{}, privilege {}, policy {} ({})",
        principal.getName(),
        metalake,
        target.type(),
        target.fullName(),
        privilege,
        policy.name(),
        policy.id());
    return true;
  }

  private void logSuppressedTagAllow(
      Principal principal,
      String metalake,
      MetadataObject target,
      Privilege.Name privilege,
      AuthorizationRequestContext context,
      MetadataObject denyScope) {
    try {
      authorizer
          .matchTagPolicy(principal, metalake, target, privilege, context)
          .ifPresent(
              policy ->
                  LOG.debug(
                      "Suppressed tag allow: principal {}, metalake {}, object {}:{}, privilege {}, "
                          + "policy {} ({}), deny scope {}:{}",
                      principal.getName(),
                      metalake,
                      target.type(),
                      target.fullName(),
                      privilege,
                      policy.name(),
                      policy.id(),
                      denyScope.type(),
                      denyScope.fullName()));
    } catch (RuntimeException e) {
      LOG.debug("Unable to resolve a tag grant for an already denied privilege", e);
    }
  }

  @Nullable
  private MetadataObject findDeny(
      Principal principal,
      String metalake,
      List<MetadataObject> scopes,
      Privilege.Name privilege,
      AuthorizationRequestContext context) {
    for (MetadataObject scope : scopes) {
      if (scope != null && authorizer.deny(principal, metalake, scope, privilege, context)) {
        return scope;
      }
    }
    return null;
  }
}
