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
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * Makes entry authorization state available to list filtering during a synchronous read request.
 * Workers receive the context explicitly; this thread-local scope is not inherited by workers.
 * Nested invocations start isolated and restore the enclosing scope when closed.
 */
public final class AuthorizationRequestScope implements AutoCloseable {
  private static final ThreadLocal<AuthorizationRequestScope> CURRENT = new ThreadLocal<>();

  @Nullable private final AuthorizationRequestScope previous;
  private final Principal principal;
  @Nullable private String metalake;
  @Nullable private GravitinoAuthorizer authorizer;
  @Nullable private AuthorizationRequestContext context;

  private AuthorizationRequestScope() {
    previous = CURRENT.get();
    principal = PrincipalUtils.getCurrentPrincipal();
    CURRENT.set(this);
  }

  /**
   * Opens an isolated invocation scope, to be closed on the calling thread with try-with-resources.
   *
   * @return the new scope
   */
  public static AuthorizationRequestScope open() {
    return new AuthorizationRequestScope();
  }

  /**
   * Binds completed entry authorization for a read-only operation to this scope.
   *
   * @param metalake the authorized metalake
   * @param authorizer the authorizer that populated the context
   * @param context the entry authorization context
   */
  public void bind(
      String metalake, GravitinoAuthorizer authorizer, AuthorizationRequestContext context) {
    this.metalake = Objects.requireNonNull(metalake, "metalake");
    this.authorizer = Objects.requireNonNull(authorizer, "authorizer");
    this.context = Objects.requireNonNull(context, "context");
  }

  /**
   * Reuses entry authorization only for the same principal instance, active roles, metalake and
   * authorizer. Otherwise returns a fresh context. Principal identity is intentional: principal
   * equality may omit active-role or authentication attributes.
   *
   * @param metalake the metalake being filtered
   * @param authorizer the authorizer used for filtering
   * @return the matching request context, or a new independent context
   */
  public static AuthorizationRequestContext getOrCreate(
      String metalake, GravitinoAuthorizer authorizer) {
    AuthorizationRequestScope scope = CURRENT.get();
    Principal principal = PrincipalUtils.getCurrentPrincipal();
    if (scope != null
        && scope.context != null
        && scope.principal == principal
        && scope.authorizer == authorizer
        && Objects.equals(scope.metalake, metalake)
        && (!(principal instanceof UserPrincipal)
            || scope
                .context
                .getActiveRoles()
                .equals(((UserPrincipal) principal).getActiveRoles()))) {
      return scope.context;
    }
    return new AuthorizationRequestContext();
  }

  /** Restores the enclosing scope or removes all state when the invocation completes. */
  @Override
  public void close() {
    if (previous == null) {
      CURRENT.remove();
    } else {
      CURRENT.set(previous);
    }
  }
}
