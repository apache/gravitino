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
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.dto.requests.UserAddRequest;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.RemoveResponse;
import org.apache.gravitino.dto.responses.UserListResponse;
import org.apache.gravitino.dto.responses.UserResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.NameBindings;
import org.apache.gravitino.server.web.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NameBindings.AccessControlInterfaces
@Path("/metalakes/{metalake}/users")
public class UserOperations {

  private static final Logger LOG = LoggerFactory.getLogger(UserOperations.class);

  private final AccessControlDispatcher accessControlManager;

  @Context private HttpServletRequest httpRequest;

  public UserOperations() {
    // Because accessManager may be null when Gravitino doesn't enable authorization,
    // and Jersey injection doesn't support null value. So UserOperations chooses to retrieve
    // accessControlManager from GravitinoEnv instead of injection here.
    this.accessControlManager = GravitinoEnv.getInstance().accessControlDispatcher();
  }

  @GET
  @Path("{user}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-user", absolute = true)
  public Response getUser(@PathParam("metalake") String metalake, @PathParam("user") String user) {
    try {
      return Utils.doAs(
          httpRequest,
          () ->
              TreeLockUtils.doWithTreeLock(
                  AuthorizationUtils.ofGroup(metalake, user),
                  LockType.READ,
                  () ->
                      Utils.ok(
                          new UserResponse(
                              DTOConverters.toDTO(accessControlManager.getUser(metalake, user))))));
    } catch (Exception e) {
      return ExceptionHandlers.handleUserException(OperationType.GET, user, metalake, e);
    }
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-user", absolute = true)
  public Response listUsers(
      @PathParam("metalake") String metalake,
      @QueryParam("details") @DefaultValue("false") boolean verbose) {
    try {
      return Utils.doAs(
          httpRequest,
          () ->
              TreeLockUtils.doWithTreeLock(
                  NameIdentifier.of(AuthorizationUtils.ofUserNamespace(metalake).levels()),
                  LockType.READ,
                  () -> {
                    if (verbose) {
                      return Utils.ok(
                          new UserListResponse(
                              DTOConverters.toDTOs(accessControlManager.listUsers(metalake))));
                    } else {
                      return Utils.ok(
                          new NameListResponse(accessControlManager.listUserNames(metalake)));
                    }
                  }));
    } catch (Exception e) {
      return ExceptionHandlers.handleUserException(OperationType.LIST, "", metalake, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "add-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "add-user", absolute = true)
  public Response addUser(@PathParam("metalake") String metalake, UserAddRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () ->
              TreeLockUtils.doWithTreeLock(
                  NameIdentifier.of(AuthorizationUtils.ofGroupNamespace(metalake).levels()),
                  LockType.WRITE,
                  () ->
                      Utils.ok(
                          new UserResponse(
                              DTOConverters.toDTO(
                                  accessControlManager.addUser(metalake, request.getName()))))));
    } catch (Exception e) {
      return ExceptionHandlers.handleUserException(
          OperationType.ADD, request.getName(), metalake, e);
    }
  }

  @DELETE
  @Path("{user}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "remove-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "remove-user", absolute = true)
  public Response removeUser(
      @PathParam("metalake") String metalake, @PathParam("user") String user) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            boolean removed =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifier.of(AuthorizationUtils.ofGroupNamespace(metalake).levels()),
                    LockType.WRITE,
                    () -> accessControlManager.removeUser(metalake, user));
            if (!removed) {
              LOG.warn("Failed to remove user {} under metalake {}", user, metalake);
            }
            return Utils.ok(new RemoveResponse(removed));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleUserException(OperationType.REMOVE, user, metalake, e);
    }
  }
}
