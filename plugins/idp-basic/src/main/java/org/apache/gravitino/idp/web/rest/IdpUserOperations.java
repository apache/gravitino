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
import java.util.List;
import javax.inject.Inject;
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
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.dto.responses.RemoveResponse;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.idp.IdpUserGroupManager;
import org.apache.gravitino.idp.dto.requests.AddUserRequest;
import org.apache.gravitino.idp.dto.requests.UpdateUserRequest;
import org.apache.gravitino.idp.dto.responses.IdpUserResponse;
import org.apache.gravitino.idp.web.IdpManagement;
import org.apache.gravitino.idp.web.IdpOperationType;
import org.apache.gravitino.idp.web.IdpRESTUtils;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.utils.PrincipalUtils;

/** REST resource for built-in IdP user management exposed by the {@code idp-basic} plugin. */
@IdpManagement
@Path("/idp/users")
public class IdpUserOperations {

  private final IdpUserGroupManager userGroupManager;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public IdpUserOperations(IdpUserGroupManager userGroupManager) {
    this.userGroupManager = userGroupManager;
  }

  @GET
  @Path("{user}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-idp-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-idp-user", absolute = true)
  public Response getUser(@PathParam("user") String user) {
    return IdpRESTUtils.doAs(
        httpRequest,
        () -> IdpRESTUtils.ok(new IdpUserResponse(userGroupManager.getUser(user).toDTO())),
        "user",
        IdpOperationType.GET,
        user);
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "add-idp-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "add-idp-user", absolute = true)
  public Response addUser(AddUserRequest request) {
    return IdpRESTUtils.doAs(
        httpRequest,
        () -> {
          request.validate();
          return IdpRESTUtils.ok(
              new IdpUserResponse(
                  userGroupManager
                      .addUser(request.getUser(), request.getPassword(), request.enabledOrDefault())
                      .toDTO()));
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
  public Response updateUser(@PathParam("user") String user, UpdateUserRequest request) {
    return IdpRESTUtils.doAs(
        httpRequest,
        () -> {
          request.validate();
          enforceSelfPasswordUpdateRules(user, request);
          if (request.getPassword() != null) {
            userGroupManager.changePassword(user, request.getPassword());
          }
          if (request.getEnabled() != null) {
            userGroupManager.updateEnabled(user, request.getEnabled());
          }
          return IdpRESTUtils.ok(new IdpUserResponse(userGroupManager.getUser(user).toDTO()));
        },
        "user",
        IdpOperationType.UPDATE,
        user);
  }

  /**
   * Non-service-admins may only change their own password and cannot update {@code enabled}.
   *
   * @param user the path username being updated
   * @param request the update request
   */
  private static void enforceSelfPasswordUpdateRules(String user, UpdateUserRequest request) {
    String currentUser = PrincipalUtils.getCurrentUserName();
    List<String> serviceAdmins = GravitinoEnv.getInstance().config().get(Configs.SERVICE_ADMINS);
    if (IdpAuthorizationFilter.isServiceAdmin(serviceAdmins, currentUser)) {
      return;
    }
    if (!user.equals(currentUser)) {
      throw new ForbiddenException(
          "Only service admins can update another user's password or enabled flag");
    }
    if (request.getEnabled() != null) {
      throw new ForbiddenException("Only service admins can update the enabled flag");
    }
  }

  @DELETE
  @Path("{user}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "remove-idp-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "remove-idp-user", absolute = true)
  public Response removeUser(@PathParam("user") String user) {
    return IdpRESTUtils.doAs(
        httpRequest,
        () -> IdpRESTUtils.ok(new RemoveResponse(userGroupManager.removeUser(user))),
        "user",
        IdpOperationType.REMOVE,
        user);
  }
}
