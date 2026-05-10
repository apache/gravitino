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
package org.apache.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.responses.RemoveResponse;
import org.apache.gravitino.idp.basic.authorization.BasicIdpManager;
import org.apache.gravitino.idp.basic.dto.requests.CreateGroupRequest;
import org.apache.gravitino.idp.basic.dto.requests.UpdateGroupUsersRequest;
import org.apache.gravitino.idp.basic.dto.responses.IdpGroupResponse;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.NameBindings;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.web.Utils;

/** REST resource for built-in IdP group management exposed by the {@code idp-basic} plugin. */
@NameBindings.AccessControlInterfaces
@Path("/idp/groups")
public class IdpGroupOperations {
  private static final String NULL_REQUEST_BODY_ERROR = "Request body cannot be null";
  private static final String SERVICE_ADMIN_ERROR =
      "Only Gravitino service admins can manage built-in IdP identities";

  private final BasicIdpManager idpManager;

  @Context private HttpServletRequest httpRequest;

  /** Creates a REST resource backed by the default built-in IdP manager. */
  public IdpGroupOperations() {
    this(new BasicIdpManager());
  }

  IdpGroupOperations(BasicIdpManager idpManager) {
    this.idpManager = idpManager;
  }

  /**
   * Gets a built-in IdP group.
   *
   * @param group the group name
   * @return the REST response
   */
  @GET
  @Path("{group}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-idp-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-idp-group", absolute = true)
  @AuthorizationExpression(expression = "SERVICE_ADMIN", errorMessage = SERVICE_ADMIN_ERROR)
  public Response getGroup(@PathParam("group") String group) {
    try {
      return Utils.doAs(
          httpRequest, () -> Utils.ok(new IdpGroupResponse(idpManager.getIdpGroup(group))));
    } catch (Exception e) {
      return ExceptionHandlers.handleIdpGroupException(OperationType.GET, group, e);
    }
  }

  /**
   * Creates a built-in IdP group.
   *
   * @param request the request body
   * @return the REST response
   */
  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "add-idp-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "add-idp-group", absolute = true)
  @AuthorizationExpression(expression = "SERVICE_ADMIN", errorMessage = SERVICE_ADMIN_ERROR)
  public Response addGroup(CreateGroupRequest request) {
    if (request == null) {
      return ExceptionHandlers.handleIdpGroupException(
          OperationType.ADD, "", new IllegalArgumentException(NULL_REQUEST_BODY_ERROR));
    }

    String group = request.getGroup();
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            return Utils.ok(new IdpGroupResponse(idpManager.createIdpGroup(request.getGroup())));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleIdpGroupException(OperationType.ADD, group, e);
    }
  }

  /**
   * Removes a built-in IdP group.
   *
   * @param group the group name
   * @param force whether to force deletion
   * @return the REST response
   */
  @DELETE
  @Path("{group}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "remove-idp-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "remove-idp-group", absolute = true)
  @AuthorizationExpression(expression = "SERVICE_ADMIN", errorMessage = SERVICE_ADMIN_ERROR)
  public Response removeGroup(
      @PathParam("group") String group, @DefaultValue("false") @QueryParam("force") boolean force) {
    try {
      return Utils.doAs(
          httpRequest, () -> Utils.ok(new RemoveResponse(idpManager.deleteGroup(group, force))));
    } catch (Exception e) {
      return ExceptionHandlers.handleIdpGroupException(OperationType.REMOVE, group, e);
    }
  }

  /**
   * Adds users to a built-in IdP group.
   *
   * @param group the group name
   * @param request the request body
   * @return the REST response
   */
  @PUT
  @Path("{group}/add")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "add-idp-group-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "add-idp-group-user", absolute = true)
  @AuthorizationExpression(expression = "SERVICE_ADMIN", errorMessage = SERVICE_ADMIN_ERROR)
  public Response addUsers(@PathParam("group") String group, UpdateGroupUsersRequest request) {
    if (request == null) {
      return ExceptionHandlers.handleIdpGroupException(
          OperationType.ADD, group, new IllegalArgumentException(NULL_REQUEST_BODY_ERROR));
    }

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            return Utils.ok(
                new IdpGroupResponse(idpManager.addUsersToIdpGroup(group, request.getUsers())));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleIdpGroupException(OperationType.ADD, group, e);
    }
  }

  /**
   * Removes users from a built-in IdP group.
   *
   * @param group the group name
   * @param request the request body
   * @return the REST response
   */
  @PUT
  @Path("{group}/remove")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "remove-idp-group-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "remove-idp-group-user", absolute = true)
  @AuthorizationExpression(expression = "SERVICE_ADMIN", errorMessage = SERVICE_ADMIN_ERROR)
  public Response removeUsers(@PathParam("group") String group, UpdateGroupUsersRequest request) {
    if (request == null) {
      return ExceptionHandlers.handleIdpGroupException(
          OperationType.REMOVE, group, new IllegalArgumentException(NULL_REQUEST_BODY_ERROR));
    }

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            return Utils.ok(
                new IdpGroupResponse(
                    idpManager.removeUsersFromIdpGroup(group, request.getUsers())));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleIdpGroupException(OperationType.REMOVE, group, e);
    }
  }
}
