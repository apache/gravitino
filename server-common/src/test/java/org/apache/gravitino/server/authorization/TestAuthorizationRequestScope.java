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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.ActiveRoles;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Test;

/** Tests security boundaries and lifetime of read-request context reuse. */
public class TestAuthorizationRequestScope {
  /** A scope must not share role loading or cached decisions across security identities. */
  @Test
  public void testSecurityBoundaries() throws Exception {
    UserPrincipal principal = new UserPrincipal("tester");
    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
    PrincipalUtils.doAs(
        principal,
        () -> {
          AuthorizationRequestContext context = new AuthorizationRequestContext();
          try (AuthorizationRequestScope scope = AuthorizationRequestScope.open()) {
            scope.bind("metalake", authorizer, context);
            assertSame(context, AuthorizationRequestScope.getOrCreate("metalake", authorizer));
            assertNotSame(context, AuthorizationRequestScope.getOrCreate("other", authorizer));
            assertNotSame(
                context,
                AuthorizationRequestScope.getOrCreate("metalake", mock(GravitinoAuthorizer.class)));
            PrincipalUtils.doAs(
                new UserPrincipal("other"),
                () -> {
                  assertNotSame(
                      context, AuthorizationRequestScope.getOrCreate("metalake", authorizer));
                  return null;
                });
            UserPrincipal assumed = principal.withActiveRoles(ActiveRoles.of(List.of("reader")));
            assertEquals(principal, assumed);
            PrincipalUtils.doAs(
                assumed,
                () -> {
                  AuthorizationRequestContext isolated =
                      AuthorizationRequestScope.getOrCreate("metalake", authorizer);
                  assertNotSame(context, isolated);
                  assertEquals(assumed.getActiveRoles(), isolated.getActiveRoles());
                  return null;
                });
            context.setActiveRoles(ActiveRoles.none());
            assertNotSame(context, AuthorizationRequestScope.getOrCreate("metalake", authorizer));
          }
          assertNotSame(context, AuthorizationRequestScope.getOrCreate("metalake", authorizer));
          return null;
        });
  }

  /** Nested and asynchronous work must not accidentally inherit cached authorization. */
  @Test
  public void testNestedScopeAndWorkerIsolation() throws Exception {
    PrincipalUtils.doAs(
        new UserPrincipal("tester"),
        () -> {
          GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
          AuthorizationRequestContext context = new AuthorizationRequestContext();
          try (AuthorizationRequestScope outer = AuthorizationRequestScope.open()) {
            outer.bind("metalake", authorizer, context);
            assertThrows(
                IllegalStateException.class,
                () -> {
                  try (AuthorizationRequestScope inner = AuthorizationRequestScope.open()) {
                    assertNotSame(
                        context, AuthorizationRequestScope.getOrCreate("metalake", authorizer));
                    AuthorizationRequestContext nested = new AuthorizationRequestContext();
                    inner.bind("metalake", authorizer, nested);
                    assertSame(
                        nested, AuthorizationRequestScope.getOrCreate("metalake", authorizer));
                    throw new IllegalStateException("nested invocation failed");
                  }
                });
            assertSame(context, AuthorizationRequestScope.getOrCreate("metalake", authorizer));
            assertNotSame(
                context,
                CompletableFuture.supplyAsync(
                        () -> AuthorizationRequestScope.getOrCreate("metalake", authorizer))
                    .join());
          }
          return null;
        });
  }
}
