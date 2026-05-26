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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.idp.IdpUserGroupManager;
import org.apache.gravitino.idp.model.IdpUser;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests that {@link IdpAuthorizationFilter} is scoped via name binding. */
class TestIdpAuthorizationNameBinding extends JerseyTest {

  private static final IdpUserGroupManager MANAGER = mock(IdpUserGroupManager.class);

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(4001, 5000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRemoteUser()).thenReturn(null);

    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
    when(authorizer.isServiceAdmin()).thenReturn(false);

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(new IdpUserOperations(MANAGER));
    resourceConfig.register(OtherOperations.class);
    resourceConfig.register(
        new IdpAuthorizationFilter(
            () -> List.of(IdpAuthorizationFilter.BASIC_AUTHENTICATOR), () -> authorizer));
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(request).to(HttpServletRequest.class);
          }
        });
    return resourceConfig;
  }

  @Test
  void testFilterAppliesOnlyToIdpManagementResources() throws Exception {
    doReturn(buildUser("alice")).when(MANAGER).getUser("alice");

    Response idpResponse =
        target("/idp/users/alice")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.FORBIDDEN.getStatusCode(), idpResponse.getStatus());
    ErrorResponse idpError = idpResponse.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.FORBIDDEN_CODE, idpError.getCode());

    Response otherResponse = target("/other/ping").request(MediaType.APPLICATION_JSON_TYPE).get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), otherResponse.getStatus());
  }

  private static IdpUser buildUser(String name) {
    return new IdpUser(name, Collections.emptyList());
  }

  @Path("/other")
  public static class OtherOperations {

    @GET
    @Path("ping")
    public Response ping() {
      return Response.ok().build();
    }
  }
}
