/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.dto.requests.UserCreateRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.UserResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.lock.LockType;
import com.datastrato.gravitino.lock.TreeLockUtils;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.server.web.Utils;
import com.datastrato.gravitino.tenant.AccessControlManager;
import javax.inject.Inject;
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

@Path("/metalakes/{metalake}/users")
public class UserOperations {

  private static final Logger LOG = LoggerFactory.getLogger(UserOperations.class);

  private final AccessControlManager accessControlManager;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public UserOperations(AccessControlManager accessControlManager) {
    this.accessControlManager = accessControlManager;
  }

  @GET
  @Path("{user}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "load-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-user", absolute = true)
  public Response loadUser(@PathParam("metalake") String metalake, @PathParam("user") String user) {
    try {
      NameIdentifier ident = NameIdentifier.of(metalake, user);
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new UserResponse(
                      DTOConverters.toDTO(
                          TreeLockUtils.doWithTreeLock(
                              ident,
                              LockType.READ,
                              () -> accessControlManager.loadUser(metalake, user))))));
    } catch (Exception e) {
      return ExceptionHandlers.handleUserException(OperationType.LOAD, user, metalake, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "create-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-user", absolute = true)
  public Response createUser(@PathParam("metalake") String metalake, UserCreateRequest request) {
    try {
      NameIdentifier ident = NameIdentifier.of(metalake, request.getName());
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new UserResponse(
                      DTOConverters.toDTO(
                          TreeLockUtils.doWithTreeLock(
                              ident,
                              LockType.WRITE,
                              () ->
                                  accessControlManager.createUser(
                                      metalake, request.getName(), request.getProperties()))))));
    } catch (Exception e) {
      return ExceptionHandlers.handleUserException(
          OperationType.CREATE, request.getName(), metalake, e);
    }
  }

  @DELETE
  @Path("{user}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-user", absolute = true)
  public Response dropUser(@PathParam("metalake") String metalake, @PathParam("user") String user) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifier.of(metalake, user);
            boolean dropped =
                TreeLockUtils.doWithTreeLock(
                    ident, LockType.WRITE, () -> accessControlManager.dropUser(metalake, user));
            if (!dropped) {
              LOG.warn("Failed to drop table {} under metalkae {}", user, metalake);
            }
            return Utils.ok(new DropResponse(dropped));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleUserException(OperationType.DROP, user, metalake, e);
    }
  }
}
