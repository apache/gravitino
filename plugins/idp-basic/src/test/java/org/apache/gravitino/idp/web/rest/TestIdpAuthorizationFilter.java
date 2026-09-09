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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.UriInfo;
import org.junit.jupiter.api.Test;

class TestIdpAuthorizationFilter {

  @Test
  void testIsServiceAdmin() {
    assertFalse(IdpAuthorizationFilter.isServiceAdmin(null, "admin"));
    assertFalse(IdpAuthorizationFilter.isServiceAdmin(List.of("admin"), null));
    assertFalse(IdpAuthorizationFilter.isServiceAdmin(List.of("admin"), "other"));
    assertTrue(IdpAuthorizationFilter.isServiceAdmin(List.of("admin", "ops"), "admin"));
  }

  @Test
  void testIsSelfUserAccess() {
    assertFalse(
        IdpAuthorizationFilter.isSelfUserAccess(request("PUT", "idp", "users", "alice"), null));
    assertFalse(
        IdpAuthorizationFilter.isSelfUserAccess(
            request("DELETE", "idp", "users", "alice"), "alice"));
    assertFalse(
        IdpAuthorizationFilter.isSelfUserAccess(request("PUT", "idp", "users", "bob"), "alice"));
    assertFalse(
        IdpAuthorizationFilter.isSelfUserAccess(request("GET", "idp", "users", "bob"), "alice"));
    assertFalse(
        IdpAuthorizationFilter.isSelfUserAccess(request("PUT", "idp", "groups", "alice"), "alice"));
    assertTrue(
        IdpAuthorizationFilter.isSelfUserAccess(request("GET", "idp", "users", "alice"), "alice"));
    assertTrue(
        IdpAuthorizationFilter.isSelfUserAccess(request("PUT", "idp", "users", "alice"), "alice"));
  }

  private static ContainerRequestContext request(String method, String... segments) {
    List<PathSegment> pathSegments =
        Arrays.stream(segments).map(TestIdpAuthorizationFilter::segment).toList();
    ContainerRequestContext context = mock(ContainerRequestContext.class);
    UriInfo uriInfo = mock(UriInfo.class);
    when(context.getMethod()).thenReturn(method);
    when(context.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPathSegments()).thenReturn(pathSegments);
    return context;
  }

  private static PathSegment segment(String path) {
    PathSegment pathSegment = mock(PathSegment.class);
    when(pathSegment.getPath()).thenReturn(path);
    return pathSegment;
  }
}
