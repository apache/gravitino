/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.authorization.AccessControlManager;
import com.datastrato.gravitino.dto.requests.UserAddRequest;
import com.datastrato.gravitino.dto.responses.RemoveResponse;
import com.datastrato.gravitino.dto.responses.UserResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.server.authorization.NameBindings;
import com.datastrato.gravitino.server.web.Utils;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NameBindings.AccessControlInterfaces
@Path("/metalakes/{metalake}/users")
public class UserOperations {

  private static final Logger LOG = LoggerFactory.getLogger(UserOperations.class);

  private final AccessControlManager accessControlManager;

  @Context private HttpServletRequest httpRequest;

  public UserOperations() {
    // Because accessManager may be null when Gravitino doesn't enable authorization,
    // and Jersey injection doesn't support null value. So UserOperations chooses to retrieve
    // accessControlManager from GravitinoEnv instead of injection here.
    this.accessControlManager = GravitinoEnv.getInstance().accessControlManager();
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
              Utils.ok(
                  new UserResponse(
                      DTOConverters.toDTO(accessControlManager.getUser(metalake, user)))));
    } catch (Exception e) {
      return ExceptionHandlers.handleUserException(OperationType.GET, user, metalake, e);
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
              Utils.ok(
                  new UserResponse(
                      DTOConverters.toDTO(
                          accessControlManager.addUser(metalake, request.getName())))));
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
            boolean removed = accessControlManager.removeUser(metalake, user);
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
