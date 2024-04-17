/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.authorization.AccessControlManager;
import com.datastrato.gravitino.authorization.Privileges;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.dto.requests.RoleCreateRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.RoleResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.server.web.Utils;
import java.util.stream.Collectors;
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

@Path("/metalakes/{metalake}/roles")
public class RoleOperations {
  private static final Logger LOG = LoggerFactory.getLogger(RoleOperations.class);

  private final AccessControlManager accessControlManager;

  @Context private HttpServletRequest httpRequest;

  public RoleOperations() {
    this.accessControlManager = GravitinoEnv.getInstance().accessControlManager();
  }

  @GET
  @Path("{role}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "load-role." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-role", absolute = true)
  public Response loadRole(@PathParam("metalake") String metalake, @PathParam("role") String role) {
    try {
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new RoleResponse(
                      DTOConverters.toDTO(accessControlManager.loadRole(metalake, role)))));
    } catch (Exception e) {
      return ExceptionHandlers.handleRoleException(OperationType.LOAD, role, metalake, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "create-role." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-role", absolute = true)
  public Response creatRole(@PathParam("metalake") String metalake, RoleCreateRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new RoleResponse(
                      DTOConverters.toDTO(
                          accessControlManager.createRole(
                              metalake,
                              request.getName(),
                              request.getProperties(),
                              SecurableObjects.parse(request.getSecurableObject()),
                              request.getPrivileges().stream()
                                  .map(Privileges::fromString)
                                  .collect(Collectors.toList()))))));
    } catch (Exception e) {
      return ExceptionHandlers.handleRoleException(
          OperationType.CREATE, request.getName(), metalake, e);
    }
  }

  @DELETE
  @Path("{role}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-role." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-role", absolute = true)
  public Response dropRole(@PathParam("metalake") String metalake, @PathParam("role") String role) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            boolean dropped = accessControlManager.dropRole(metalake, role);
            if (!dropped) {
              LOG.warn("Failed to drop role {} under metalake {}", role, metalake);
            }
            return Utils.ok(new DropResponse(dropped));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleRoleException(OperationType.DROP, role, metalake, e);
    }
  }
}
