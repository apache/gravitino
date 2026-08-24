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
package org.apache.gravitino.lance.service;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.security.Principal;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.ActiveRoles;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLanceServiceIdentityFilter {

  @Test
  public void testPreservesAuthenticatedCallerAndActiveRoles() throws Exception {
    LanceServiceIdentityFilter filter = new LanceServiceIdentityFilter("lance_rest_service_user");
    ServletRequest request = mock(ServletRequest.class);
    ServletResponse response = mock(ServletResponse.class);
    ActiveRoles activeRoles = ActiveRoles.of(List.of("analyst"));
    UserPrincipal caller = new UserPrincipal("request_user").withActiveRoles(activeRoles);
    AtomicReference<Principal> principalInChain = new AtomicReference<>();
    FilterChain chain =
        (servletRequest, servletResponse) ->
            principalInChain.set(PrincipalUtils.getCurrentPrincipal());

    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, PrincipalUtils.getCurrentUserName());

    PrincipalUtils.doAs(
        caller,
        () -> {
          filter.doFilter(request, response, chain);
          Assertions.assertSame(caller, PrincipalUtils.getCurrentPrincipal());
          return null;
        });

    Assertions.assertSame(caller, principalInChain.get());
    Assertions.assertEquals(activeRoles, ((UserPrincipal) principalInChain.get()).getActiveRoles());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, PrincipalUtils.getCurrentUserName());
  }

  @Test
  public void testBindsConfiguredServiceIdentityForAnonymousRequest() throws Exception {
    String userName = "lance_rest_service_user";
    LanceServiceIdentityFilter filter = new LanceServiceIdentityFilter(userName);
    ServletRequest request = mock(ServletRequest.class);
    ServletResponse response = mock(ServletResponse.class);
    AtomicReference<String> userInChain = new AtomicReference<>();
    FilterChain chain =
        (servletRequest, servletResponse) -> userInChain.set(PrincipalUtils.getCurrentUserName());

    filter.doFilter(request, response, chain);

    Assertions.assertEquals(userName, userInChain.get());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, PrincipalUtils.getCurrentUserName());
  }

  @Test
  public void testRestoresAnonymousIdentityAfterCheckedException() {
    LanceServiceIdentityFilter filter = new LanceServiceIdentityFilter("lance_rest_service_user");
    ServletRequest request = mock(ServletRequest.class);
    ServletResponse response = mock(ServletResponse.class);
    IOException failure = new IOException("expected failure");
    FilterChain chain =
        (servletRequest, servletResponse) -> {
          Assertions.assertEquals("lance_rest_service_user", PrincipalUtils.getCurrentUserName());
          throw failure;
        };

    IOException thrown =
        Assertions.assertThrows(IOException.class, () -> filter.doFilter(request, response, chain));

    Assertions.assertSame(failure, thrown);
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, PrincipalUtils.getCurrentUserName());
  }

  @Test
  public void testPreservesAuthenticatedCallerAfterUnexpectedException() throws Exception {
    LanceServiceIdentityFilter filter = new LanceServiceIdentityFilter("lance_rest_service_user");
    ServletRequest request = mock(ServletRequest.class);
    ServletResponse response = mock(ServletResponse.class);
    UserPrincipal outerCaller = new UserPrincipal("outer_user");
    RuntimeException failure = new RuntimeException("expected failure");
    FilterChain chain =
        (servletRequest, servletResponse) -> {
          Assertions.assertSame(outerCaller, PrincipalUtils.getCurrentPrincipal());
          throw failure;
        };

    PrincipalUtils.doAs(
        outerCaller,
        () -> {
          ServletException thrown =
              Assertions.assertThrows(
                  ServletException.class, () -> filter.doFilter(request, response, chain));
          Assertions.assertSame(failure, thrown.getCause());
          Assertions.assertEquals("Failed to execute the Lance REST request", thrown.getMessage());
          Assertions.assertSame(outerCaller, PrincipalUtils.getCurrentPrincipal());
          return null;
        });

    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, PrincipalUtils.getCurrentUserName());
  }
}
