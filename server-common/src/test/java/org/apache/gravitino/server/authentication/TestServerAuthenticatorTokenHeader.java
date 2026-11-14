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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.Vector;
import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestServerAuthenticatorTokenHeader {

  private static final String CUSTOM_HEADER = "X-Custom-Token";

  @AfterEach
  public void resetTokenHeader() {
    // ServerAuthenticator is a singleton, restore the default for other tests.
    ServerAuthenticator.getInstance().initialize(new Config(false) {});
  }

  @Test
  public void testTokenHeaderDefaultsToAuthorization() {
    ServerAuthenticator.getInstance().initialize(new Config(false) {});
    Assertions.assertEquals(
        AuthConstants.HTTP_HEADER_AUTHORIZATION, ServerAuthenticator.getInstance().tokenHeader());
  }

  @Test
  public void testTokenHeaderIsConfigurable() {
    ServerAuthenticator.getInstance().initialize(configWithHeader(CUSTOM_HEADER));
    Assertions.assertEquals(CUSTOM_HEADER, ServerAuthenticator.getInstance().tokenHeader());
  }

  @Test
  public void testBlankTokenHeaderIsRejected() {
    Config config = configWithHeader(" ");
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> config.get(Configs.AUTHENTICATOR_TOKEN_HEADER));
  }

  @Test
  public void testFilterReadsCustomHeader() throws Exception {
    ServerAuthenticator.getInstance().initialize(configWithHeader(CUSTOM_HEADER));

    Authenticator authenticator = mock(Authenticator.class);
    AuthenticationFilter filter = new AuthenticationFilter(Lists.newArrayList(authenticator));
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);
    when(request.getHeaders(CUSTOM_HEADER))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(request.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(Collections.emptyEnumeration());
    when(authenticator.supportsToken(any())).thenReturn(true);
    when(authenticator.isDataFromToken()).thenReturn(true);
    when(authenticator.authenticateToken(any())).thenReturn(new UserPrincipal("user"));

    filter.doFilter(request, response, chain);

    verify(response, never()).sendError(anyInt(), anyString());
    verify(chain).doFilter(any(), any());
  }

  private static Config configWithHeader(String header) {
    Config config = new Config(false) {};
    config.set(Configs.AUTHENTICATOR_TOKEN_HEADER, header);
    return config;
  }
}
