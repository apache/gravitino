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
package org.apache.gravitino.paimon.service;

import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.KeyPair;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.Configs;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.authentication.OAuthConfig;
import org.apache.gravitino.server.authentication.ServerAuthenticator;
import org.apache.paimon.rest.exceptions.NotAuthorizedException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPaimonAuthenticationFilter {

  @Test
  void testDelegatedOAuthValidationAndCallerIdentity() throws Exception {
    KeyPair key = Keys.keyPairFor(SignatureAlgorithm.RS256);
    ServerConfig config = new ServerConfig(false);
    config.set(Configs.AUTHENTICATORS, List.of("oauth"));
    config.set(OAuthConfig.SERVICE_AUDIENCE, "paimon");
    config.set(
        OAuthConfig.DEFAULT_SIGN_KEY,
        Base64.getEncoder().encodeToString(key.getPublic().getEncoded()));
    config.set(OAuthConfig.SIGNATURE_ALGORITHM_TYPE, "RS256");
    config.set(OAuthConfig.DEFAULT_SERVER_URI, "http://localhost");
    config.set(OAuthConfig.DEFAULT_TOKEN_PATH, "token");
    ServerAuthenticator authenticators = ServerAuthenticator.getInstance();
    authenticators.initialize(config);
    try {
      PaimonAuthenticationFilter filter = new PaimonAuthenticationFilter();
      HttpServletRequest request = oauthRequest(jwt(key, Instant.now().plusSeconds(300)));
      HttpServletResponse response = mock(HttpServletResponse.class);
      FilterChain chain = mock(FilterChain.class);
      filter.doFilter(request, response, chain);
      verify(chain).doFilter(request, response);
      verify(request)
          .setAttribute(
              eq(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME),
              argThat(
                  principal ->
                      principal instanceof UserPrincipal
                          && ((UserPrincipal) principal).getName().equals("flink-job")));

      for (String token :
          new String[] {null, "invalid", jwt(key, Instant.now().minusSeconds(300))}) {
        HttpServletRequest invalidRequest = oauthRequest(token);
        HttpServletResponse rejected = mock(HttpServletResponse.class);
        StringWriter body = new StringWriter();
        when(rejected.getWriter()).thenReturn(new PrintWriter(body));
        filter.doFilter(invalidRequest, rejected, chain);
        verify(chain, never()).doFilter(invalidRequest, rejected);
        verify(rejected).setStatus(401);
        Assertions.assertTrue(body.toString().contains("\"code\":401"));
      }
    } finally {
      authenticators.initialize(new ServerConfig());
    }
  }

  private static HttpServletRequest oauthRequest(String token) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    List<String> headers = token == null ? List.of() : List.of("Bearer " + token);
    when(request.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(Collections.enumeration(headers));
    return request;
  }

  private static String jwt(KeyPair key, Instant expiresAt) {
    return Jwts.builder()
        .setAudience("paimon")
        .setSubject("flink-job")
        .setExpiration(Date.from(expiresAt))
        .signWith(key.getPrivate(), SignatureAlgorithm.RS256)
        .compact();
  }

  @Test
  public void testSharedTokenValidationAcceptsMatchingToken() throws Exception {
    PaimonAuthenticationFilter filter = new PaimonAuthenticationFilter("secret");
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);
    when(request.getHeader("Authorization")).thenReturn("Bearer secret");

    filter.doFilter(request, response, chain);

    verify(chain).doFilter(request, response);
  }

  @Test
  public void testSharedTokenValidationRejectsWrongToken() throws Exception {
    PaimonAuthenticationFilter filter = new PaimonAuthenticationFilter("secret");
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);
    StringWriter body = new StringWriter();
    when(request.getHeader("Authorization")).thenReturn("Bearer wrong");
    when(response.getWriter()).thenReturn(new PrintWriter(body, true));

    filter.doFilter(request, response, chain);

    verify(chain, org.mockito.Mockito.never()).doFilter(request, response);
    verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    Assertions.assertTrue(body.toString().contains("\"code\":401"));
  }

  @Test
  public void testSharedTokenValidationRejectsMissingHeader() throws Exception {
    PaimonAuthenticationFilter filter = new PaimonAuthenticationFilter("secret");
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);
    StringWriter body = new StringWriter();
    when(request.getHeader("Authorization")).thenReturn(null);
    when(response.getWriter()).thenReturn(new PrintWriter(body, true));

    filter.doFilter(request, response, chain);

    verify(chain, org.mockito.Mockito.never()).doFilter(request, response);
    verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  public void testPerPrincipalTokenAuthenticatesIdentity() throws Exception {
    PaimonAuthenticationFilter filter =
        new PaimonAuthenticationFilter(
            null, java.util.Collections.singletonMap("flink-prod", "token-a"));
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);
    when(request.getHeader("Authorization")).thenReturn("Bearer token-a");

    filter.doFilter(request, response, chain);

    org.mockito.ArgumentCaptor<Object> principalCaptor =
        org.mockito.ArgumentCaptor.forClass(Object.class);
    verify(request)
        .setAttribute(
            org.mockito.Mockito.eq(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME),
            principalCaptor.capture());
    Assertions.assertEquals(
        "flink-prod", ((org.apache.gravitino.UserPrincipal) principalCaptor.getValue()).getName());
    verify(chain).doFilter(request, response);
  }

  @Test
  public void testPerPrincipalTokenRejectsUnknownToken() throws Exception {
    PaimonAuthenticationFilter filter =
        new PaimonAuthenticationFilter(
            null, java.util.Collections.singletonMap("flink-prod", "token-a"));
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);
    StringWriter body = new StringWriter();
    when(request.getHeader("Authorization")).thenReturn("Bearer someone-elses-token");
    when(response.getWriter()).thenReturn(new PrintWriter(body, true));

    filter.doFilter(request, response, chain);

    verify(chain, org.mockito.Mockito.never()).doFilter(request, response);
    verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  public void testAuthErrorResponseIsPaimonJson() throws Exception {
    PaimonAuthenticationFilter filter = new PaimonAuthenticationFilter();
    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter body = new StringWriter();
    when(response.getWriter()).thenReturn(new PrintWriter(body, true));

    filter.sendAuthErrorResponse(response, new NotAuthorizedException("bad token"));

    verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    verify(response).setContentType("application/json");
    String json = body.toString();
    Assertions.assertTrue(json.contains("\"code\":401"), "body should carry 401 code: " + json);
    Assertions.assertTrue(json.contains("Not authorized"), "body should carry message: " + json);
  }
}
