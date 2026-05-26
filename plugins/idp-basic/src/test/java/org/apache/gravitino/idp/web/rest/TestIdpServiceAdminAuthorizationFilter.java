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
package org.apache.gravitino.idp.web.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestIdpServiceAdminAuthorizationFilter {

  @Test
  void testAllowsServiceAdminOnIdpPath() throws Exception {
    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
    when(authorizer.isServiceAdmin()).thenReturn(true);

    IdpServiceAdminAuthorizationFilter filter =
        new IdpServiceAdminAuthorizationFilter(
            () -> Collections.singletonList(IdpServiceAdminAuthorizationFilter.BASIC_AUTHENTICATOR),
            () -> authorizer);
    ContainerRequestContext requestContext = mockRequestContext("idp/users/alice");

    filter.filter(requestContext);

    verify(requestContext, never()).abortWith(any(Response.class));
    verify(authorizer).isServiceAdmin();
  }

  @Test
  void testRejectsNonServiceAdminOnIdpPath() throws Exception {
    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
    when(authorizer.isServiceAdmin()).thenReturn(false);

    IdpServiceAdminAuthorizationFilter filter =
        new IdpServiceAdminAuthorizationFilter(
            () -> Collections.singletonList(IdpServiceAdminAuthorizationFilter.BASIC_AUTHENTICATOR),
            () -> authorizer);
    ContainerRequestContext requestContext = mockRequestContext("idp/groups/engineering");

    filter.filter(requestContext);

    Response response = captureAbortedResponse(requestContext);
    Assertions.assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    Assertions.assertEquals(ErrorConstants.FORBIDDEN_CODE, errorResponse.getCode());
    Assertions.assertEquals(
        IdpServiceAdminAuthorizationFilter.SERVICE_ADMIN_REQUIRED_MESSAGE,
        errorResponse.getMessage());
  }

  @Test
  void testRejectsWhenBasicAuthenticatorDisabled() throws Exception {
    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);

    IdpServiceAdminAuthorizationFilter filter =
        new IdpServiceAdminAuthorizationFilter(Collections::emptyList, () -> authorizer);
    ContainerRequestContext requestContext = mockRequestContext("idp/users");

    filter.filter(requestContext);

    Response response = captureAbortedResponse(requestContext);
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(
        IdpServiceAdminAuthorizationFilter.BASIC_AUTHENTICATOR_REQUIRED_MESSAGE,
        errorResponse.getMessage());
    verify(authorizer, never()).isServiceAdmin();
  }

  @Test
  void testSkipsNonIdpPaths() throws Exception {
    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);

    IdpServiceAdminAuthorizationFilter filter =
        new IdpServiceAdminAuthorizationFilter(
            () -> Collections.singletonList(IdpServiceAdminAuthorizationFilter.BASIC_AUTHENTICATOR),
            () -> authorizer);
    ContainerRequestContext requestContext = mockRequestContext("metalakes");

    filter.filter(requestContext);

    verify(requestContext, never()).abortWith(any(Response.class));
    verify(authorizer, never()).isServiceAdmin();
  }

  private static ContainerRequestContext mockRequestContext(String path) {
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    javax.ws.rs.core.UriInfo uriInfo = mock(javax.ws.rs.core.UriInfo.class);
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPath()).thenReturn(path);
    return requestContext;
  }

  private static Response captureAbortedResponse(ContainerRequestContext requestContext) {
    org.mockito.ArgumentCaptor<Response> responseCaptor =
        org.mockito.ArgumentCaptor.forClass(Response.class);
    verify(requestContext).abortWith(responseCaptor.capture());
    return responseCaptor.getValue();
  }
}
