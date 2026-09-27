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

import java.lang.reflect.Method;
import java.security.Principal;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * Makes entry authorization state available to list filtering during a synchronous read request.
 *
 * <p>The interceptor opens a scope around the REST method, binds the context it built for entry
 * authorization when the method is a read, and closes the scope when the method returns. {@link
 * #getOrCreate(String)} then hands that context to {@code MetadataAuthzHelper.filterByExpression}
 * on the same thread. Filter workers receive the context explicitly; this thread-local scope is not
 * inherited by them.
 *
 * <p>This is separate from {@link org.apache.gravitino.utils.RequestContext} because it is bound to
 * the intercepted method, not to the servlet request, and is closed together with it.
 */
public final class AuthorizationRequestScope implements AutoCloseable {
  private static final ThreadLocal<AuthorizationRequestScope> CURRENT = new ThreadLocal<>();

  @Nullable private Principal principal;
  @Nullable private String metalake;
  @Nullable private AuthorizationRequestContext context;

  private AuthorizationRequestScope() {}

  /**
   * Opens the scope of one intercepted invocation, to be closed on the same thread with
   * try-with-resources.
   *
   * @return the new scope
   */
  public static AuthorizationRequestScope open() {
    AuthorizationRequestScope scope = new AuthorizationRequestScope();
    CURRENT.set(scope);
    return scope;
  }

  /**
   * Binds completed entry authorization to this scope when the method is a read operation. Only
   * reads may reuse entry decisions, because a mutation could invalidate them before the list is
   * filtered. Nothing is bound for other methods or when no metalake was authorized.
   *
   * @param method the intercepted REST method
   * @param metalakeIdent the authorized metalake, or null when entry authorization had none
   * @param context the entry authorization context
   */
  public void bindIfRead(
      Method method, @Nullable NameIdentifier metalakeIdent, AuthorizationRequestContext context) {
    if (metalakeIdent != null && method.isAnnotationPresent(GET.class)) {
      bind(metalakeIdent.name(), context);
    }
  }

  /**
   * Binds completed entry authorization for a read-only operation to this scope.
   *
   * @param metalake the authorized metalake
   * @param context the entry authorization context
   */
  public void bind(String metalake, AuthorizationRequestContext context) {
    this.principal = PrincipalUtils.getCurrentPrincipal();
    this.metalake = Objects.requireNonNull(metalake, "metalake");
    this.context = Objects.requireNonNull(context, "context");
  }

  /**
   * Returns the bound entry context when it was built for the current principal instance and the
   * given metalake, otherwise a fresh context. Principal identity is compared on purpose: a context
   * snapshots the principal's active roles when it is created, and principal equality may ignore
   * those.
   *
   * @param metalake the metalake being filtered
   * @return the matching request context, or a new independent context
   */
  public static AuthorizationRequestContext getOrCreate(String metalake) {
    AuthorizationRequestScope scope = CURRENT.get();
    if (scope != null
        && scope.context != null
        && scope.principal == PrincipalUtils.getCurrentPrincipal()
        && Objects.equals(scope.metalake, metalake)) {
      return scope.context;
    }
    return new AuthorizationRequestContext();
  }

  /** Removes the scope when the invocation completes. */
  @Override
  public void close() {
    CURRENT.remove();
  }
}
