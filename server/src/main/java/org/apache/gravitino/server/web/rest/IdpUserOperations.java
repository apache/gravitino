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
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.authorization.IdpUserManager;
import org.apache.gravitino.dto.requests.CreateUserRequest;
import org.apache.gravitino.dto.requests.ResetPasswordRequest;
import org.apache.gravitino.dto.responses.IdpUserResponse;
import org.apache.gravitino.dto.responses.RemoveResponse;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.web.Utils;

@Path("/idp/users")
public class IdpUserOperations {

  private final IdpUserManager userManager;

  @Context private HttpServletRequest httpRequest;

  public IdpUserOperations() {
    this(IdpUserManager.fromEnvironment());
  }

  IdpUserOperations(IdpUserManager userManager) {
    this.userManager = userManager;
  }

  @GET
  @Path("{user}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-idp-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-idp-user", absolute = true)
  public Response getUser(@PathParam("user") String user) {
    try {
      return Utils.doAs(
          httpRequest, () -> Utils.ok(new IdpUserResponse(userManager.getUser(user))));
    } catch (Exception e) {
      return ExceptionHandlers.handleUserException(OperationType.GET, user, "", e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "add-idp-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "add-idp-user", absolute = true)
  public Response addUser(CreateUserRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            return Utils.ok(
                new IdpUserResponse(
                    userManager.createUser(request.getUser(), request.getPassword())));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleUserException(OperationType.ADD, request.getUser(), "", e);
    }
  }

  @PUT
  @Path("{user}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "update-idp-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "update-idp-user", absolute = true)
  public Response resetPassword(@PathParam("user") String user, ResetPasswordRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            return Utils.ok(
                new IdpUserResponse(userManager.resetPassword(user, request.getPassword())));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleUserException(OperationType.UPDATE, user, "", e);
    }
  }

  @DELETE
  @Path("{user}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "remove-idp-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "remove-idp-user", absolute = true)
  public Response removeUser(@PathParam("user") String user) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            boolean removed = userManager.deleteUser(user);
            return Utils.ok(new RemoveResponse(removed));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleUserException(OperationType.REMOVE, user, "", e);
    }
  }
}
