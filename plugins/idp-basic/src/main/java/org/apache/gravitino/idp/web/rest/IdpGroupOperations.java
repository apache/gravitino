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
import java.util.Arrays;
import javax.inject.Inject;
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
import org.apache.gravitino.idp.IdpUserGroupManager;
import org.apache.gravitino.idp.dto.requests.AddGroupRequest;
import org.apache.gravitino.idp.dto.requests.GroupMembershipChangeRequest;
import org.apache.gravitino.idp.dto.responses.IdpGroupResponse;
import org.apache.gravitino.idp.web.IdpManagement;
import org.apache.gravitino.idp.web.IdpOperationType;
import org.apache.gravitino.idp.web.IdpRESTUtils;
import org.apache.gravitino.metrics.MetricNames;

/** REST resource for built-in IdP group management exposed by the {@code idp-basic} plugin. */
@IdpManagement
@Path("/idp/groups")
public class IdpGroupOperations {

  private final IdpUserGroupManager userGroupManager;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public IdpGroupOperations(IdpUserGroupManager userGroupManager) {
    this.userGroupManager = userGroupManager;
  }

  @GET
  @Path("{group}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-idp-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-idp-group", absolute = true)
  public Response getGroup(@PathParam("group") String group) {
    return IdpRESTUtils.doAs(
        httpRequest,
        () -> IdpRESTUtils.ok(new IdpGroupResponse(userGroupManager.getGroup(group).toDTO())),
        "group",
        IdpOperationType.GET,
        group);
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "add-idp-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "add-idp-group", absolute = true)
  public Response addGroup(AddGroupRequest request) {
    return IdpRESTUtils.doAs(
        httpRequest,
        () -> {
          request.validate();
          return IdpRESTUtils.ok(
              new IdpGroupResponse(userGroupManager.addGroup(request.getGroup()).toDTO()));
        },
        "group",
        IdpOperationType.ADD,
        request.getGroup());
  }

  @DELETE
  @Path("{group}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "remove-idp-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "remove-idp-group", absolute = true)
  public Response removeGroup(
      @PathParam("group") String group, @DefaultValue("false") @QueryParam("force") boolean force) {
    return IdpRESTUtils.doAs(
        httpRequest,
        () -> IdpRESTUtils.ok(new RemoveResponse(userGroupManager.removeGroup(group, force))),
        "group",
        IdpOperationType.REMOVE,
        group);
  }

  @PUT
  @Path("{group}/users")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "change-idp-group-users." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "change-idp-group-users", absolute = true)
  public Response changeGroupMembership(
      @PathParam("group") String group, GroupMembershipChangeRequest request) {
    return IdpRESTUtils.doAs(
        httpRequest,
        () -> {
          request.validate();
          String[] usersToAdd = request.getUsersToAdd();
          String[] usersToRemove = request.getUsersToRemove();
          return IdpRESTUtils.ok(
              new IdpGroupResponse(
                  userGroupManager
                      .changeGroupMembership(
                          group,
                          usersToAdd == null ? null : Arrays.asList(usersToAdd),
                          usersToRemove == null ? null : Arrays.asList(usersToRemove))
                      .toDTO()));
        },
        "group",
        IdpOperationType.UPDATE,
        group);
  }
}
