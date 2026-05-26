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
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.dto.responses.RemoveResponse;
import org.apache.gravitino.idp.IdpUserGroupManager;
import org.apache.gravitino.idp.dto.requests.AddGroupRequest;
import org.apache.gravitino.idp.dto.requests.GroupMembershipChangeRequest;
import org.apache.gravitino.idp.dto.responses.IdpGroupResponse;
import org.apache.gravitino.idp.dto.util.IdpDTOConverters;
import org.apache.gravitino.idp.model.IdpGroup;
import org.apache.gravitino.idp.web.IdpManagement;
import org.apache.gravitino.idp.web.IdpOperationType;
import org.apache.gravitino.idp.web.IdpRestUtils;
import org.apache.gravitino.metrics.MetricNames;

/** REST resource for built-in IdP group management exposed by the {@code idp-basic} plugin. */
@IdpManagement
@Path("/idp/groups")
public class IdpGroupOperations {

  private final IdpUserGroupManager userGroupManager;

  @Context private HttpServletRequest httpRequest;

  public IdpGroupOperations() {
    this(
        new IdpUserGroupManager(
            GravitinoEnv.getInstance().config(), GravitinoEnv.getInstance().idGenerator()));
  }

  IdpGroupOperations(IdpUserGroupManager userGroupManager) {
    this.userGroupManager = userGroupManager;
  }

  @GET
  @Path("{group}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-idp-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-idp-group", absolute = true)
  public Response getGroup(@PathParam("group") String group) {
    try {
      return IdpRestUtils.doAs(
          httpRequest,
          () ->
              IdpRestUtils.ok(
                  new IdpGroupResponse(IdpDTOConverters.toDTO(userGroupManager.getGroup(group)))));
    } catch (Exception e) {
      return IdpRestUtils.handleException("group", IdpOperationType.GET, group, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "add-idp-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "add-idp-group", absolute = true)
  public Response addGroup(AddGroupRequest request) {
    try {
      return IdpRestUtils.doAs(
          httpRequest,
          () -> {
            request.validate();
            return IdpRestUtils.ok(
                new IdpGroupResponse(
                    IdpDTOConverters.toDTO(userGroupManager.addGroup(request.getGroup()))));
          });
    } catch (Exception e) {
      return IdpRestUtils.handleException("group", IdpOperationType.ADD, request.getGroup(), e);
    }
  }

  @DELETE
  @Path("{group}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "remove-idp-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "remove-idp-group", absolute = true)
  public Response removeGroup(
      @PathParam("group") String group, @DefaultValue("false") @QueryParam("force") boolean force) {
    try {
      return IdpRestUtils.doAs(
          httpRequest,
          () -> IdpRestUtils.ok(new RemoveResponse(userGroupManager.removeGroup(group, force))));
    } catch (Exception e) {
      return IdpRestUtils.handleException("group", IdpOperationType.REMOVE, group, e);
    }
  }

  @PUT
  @Path("{group}/add")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "add-idp-group-users." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "add-idp-group-users", absolute = true)
  public Response addUsersToGroup(
      @PathParam("group") String group, GroupMembershipChangeRequest request) {
    try {
      return IdpRestUtils.doAs(
          httpRequest,
          () -> {
            request.validate();
            String[] usersToAdd = request.getUsersToAdd();
            IdpGroup groupEntity =
                userGroupManager.changeGroupMembership(
                    group, usersToAdd == null ? null : Arrays.asList(usersToAdd), null);
            return IdpRestUtils.ok(new IdpGroupResponse(IdpDTOConverters.toDTO(groupEntity)));
          });
    } catch (Exception e) {
      return IdpRestUtils.handleException("group", IdpOperationType.ADD, group, e);
    }
  }

  @PUT
  @Path("{group}/remove")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "remove-idp-group-users." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "remove-idp-group-users", absolute = true)
  public Response removeUsersFromGroup(
      @PathParam("group") String group, GroupMembershipChangeRequest request) {
    try {
      return IdpRestUtils.doAs(
          httpRequest,
          () -> {
            request.validate();
            String[] usersToRemove = request.getUsersToRemove();
            IdpGroup groupEntity =
                userGroupManager.changeGroupMembership(
                    group, null, usersToRemove == null ? null : Arrays.asList(usersToRemove));
            return IdpRestUtils.ok(new IdpGroupResponse(IdpDTOConverters.toDTO(groupEntity)));
          });
    } catch (Exception e) {
      return IdpRestUtils.handleException("group", IdpOperationType.REMOVE, group, e);
    }
  }
}
