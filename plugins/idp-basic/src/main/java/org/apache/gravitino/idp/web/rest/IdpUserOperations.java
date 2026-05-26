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

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.dto.responses.RemoveResponse;
import org.apache.gravitino.idp.IdpUserGroupManager;
import org.apache.gravitino.idp.dto.requests.AddUserRequest;
import org.apache.gravitino.idp.dto.requests.ResetPasswordRequest;
import org.apache.gravitino.idp.dto.responses.IdpUserResponse;
import org.apache.gravitino.idp.dto.util.IdpDTOConverters;
import org.apache.gravitino.idp.exception.NotFoundException;
import org.apache.gravitino.idp.web.IdpManagement;
import org.apache.gravitino.idp.web.IdpOperationType;
import org.apache.gravitino.idp.web.IdpRestUtils;
import org.apache.gravitino.metrics.MetricNames;

/** REST resource for built-in IdP user management exposed by the {@code idp-basic} plugin. */
@IdpManagement
@Path("/idp/users")
public class IdpUserOperations {

  private final IdpUserGroupManager userGroupManager;

  @Context private HttpServletRequest httpRequest;

  public IdpUserOperations() {
    this(
        new IdpUserGroupManager(
            GravitinoEnv.getInstance().config(), GravitinoEnv.getInstance().idGenerator()));
  }

  IdpUserOperations(IdpUserGroupManager userGroupManager) {
    this.userGroupManager = userGroupManager;
  }

  @GET
  @Path("{user}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-idp-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-idp-user", absolute = true)
  public Response getUser(@PathParam("user") String user) {
    return IdpRestUtils.doAs(
        httpRequest,
        () ->
            IdpRestUtils.ok(
                new IdpUserResponse(IdpDTOConverters.toDTO(userGroupManager.getUser(user)))),
        "user",
        IdpOperationType.GET,
        user);
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "add-idp-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "add-idp-user", absolute = true)
  public Response addUser(AddUserRequest request) {
    return IdpRestUtils.doAs(
        httpRequest,
        () -> {
          request.validate();
          return IdpRestUtils.ok(
              new IdpUserResponse(
                  IdpDTOConverters.toDTO(
                      userGroupManager.addUser(request.getUser(), request.getPassword()))));
        },
        "user",
        IdpOperationType.ADD,
        request.getUser());
  }

  @PUT
  @Path("{user}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "update-idp-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "update-idp-user", absolute = true)
  public Response resetPassword(@PathParam("user") String user, ResetPasswordRequest request) {
    return IdpRestUtils.doAs(
        httpRequest,
        () -> {
          request.validate();
          if (!userGroupManager.changePassword(user, request.getPassword())) {
            throw new NotFoundException("IdP user %s does not exist", user);
          }
          return IdpRestUtils.ok(
              new IdpUserResponse(IdpDTOConverters.toDTO(userGroupManager.getUser(user))));
        },
        "user",
        IdpOperationType.UPDATE,
        user);
  }

  @DELETE
  @Path("{user}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "remove-idp-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "remove-idp-user", absolute = true)
  public Response removeUser(@PathParam("user") String user) {
    return IdpRestUtils.doAs(
        httpRequest,
        () -> IdpRestUtils.ok(new RemoveResponse(userGroupManager.removeUser(user))),
        "user",
        IdpOperationType.REMOVE,
        user);
  }
}
