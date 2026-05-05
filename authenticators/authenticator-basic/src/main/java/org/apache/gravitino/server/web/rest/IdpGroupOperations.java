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
import org.apache.gravitino.auth.local.IdpGroupManager;
import org.apache.gravitino.dto.requests.CreateGroupRequest;
import org.apache.gravitino.dto.requests.UpdateGroupUsersRequest;
import org.apache.gravitino.dto.responses.IdpGroupResponse;
import org.apache.gravitino.dto.responses.RemoveResponse;
import org.apache.gravitino.server.web.Utils;

@Path("/idp/groups")
public class IdpGroupOperations {

  private final IdpGroupManager groupManager;

  @Context private HttpServletRequest httpRequest;

  public IdpGroupOperations() {
    this(IdpGroupManager.fromEnvironment());
  }

  IdpGroupOperations(IdpGroupManager groupManager) {
    this.groupManager = groupManager;
  }

  @GET
  @Path("{group}")
  @Produces("application/vnd.gravitino.v1+json")
  public Response getGroup(@PathParam("group") String group) {
    try {
      return Utils.doAs(
          httpRequest, () -> Utils.ok(new IdpGroupResponse(groupManager.getGroup(group))));
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupException(OperationType.GET, group, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  public Response addGroup(CreateGroupRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            return Utils.ok(new IdpGroupResponse(groupManager.createGroup(request.getGroup())));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupException(OperationType.ADD, request.getGroup(), e);
    }
  }

  @DELETE
  @Path("{group}")
  @Produces("application/vnd.gravitino.v1+json")
  public Response removeGroup(
      @PathParam("group") String group, @DefaultValue("false") @QueryParam("force") boolean force) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            boolean removed = groupManager.deleteGroup(group, force);
            return Utils.ok(new RemoveResponse(removed));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupException(OperationType.REMOVE, group, e);
    }
  }

  @PUT
  @Path("{group}/add")
  @Produces("application/vnd.gravitino.v1+json")
  public Response addUsers(@PathParam("group") String group, UpdateGroupUsersRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            return Utils.ok(
                new IdpGroupResponse(groupManager.addUsersToGroup(group, request.getUsers())));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupException(OperationType.ADD, group, e);
    }
  }

  @PUT
  @Path("{group}/remove")
  @Produces("application/vnd.gravitino.v1+json")
  public Response removeUsers(@PathParam("group") String group, UpdateGroupUsersRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            return Utils.ok(
                new IdpGroupResponse(groupManager.removeUsersFromGroup(group, request.getUsers())));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupException(OperationType.REMOVE, group, e);
    }
  }
}
