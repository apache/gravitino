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

import java.util.concurrent.atomic.AtomicReference;
import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLanceServiceIdentityFilter {

  @Test
  public void testBindsConfiguredServiceIdentity() throws Exception {
    String userName = "lance_rest_service_user";
    LanceServiceIdentityFilter filter = new LanceServiceIdentityFilter(userName);
    ServletRequest request = mock(ServletRequest.class);
    ServletResponse response = mock(ServletResponse.class);
    AtomicReference<String> userInChain = new AtomicReference<>();
    FilterChain chain =
        (servletRequest, servletResponse) -> userInChain.set(PrincipalUtils.getCurrentUserName());

    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, PrincipalUtils.getCurrentUserName());

    PrincipalUtils.doAs(
        new UserPrincipal("request_user"),
        () -> {
          filter.doFilter(request, response, chain);
          return null;
        });

    Assertions.assertEquals(userName, userInChain.get());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, PrincipalUtils.getCurrentUserName());
  }
}
