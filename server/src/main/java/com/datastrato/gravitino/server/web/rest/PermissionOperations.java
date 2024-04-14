/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.authorization.AccessControlManager;
import com.datastrato.gravitino.dto.requests.RoleGrantRequest;
import com.datastrato.gravitino.dto.responses.GrantResponse;
import com.datastrato.gravitino.dto.responses.RemoveResponse;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.server.web.Utils;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

@Path("/metalakes/{metalake}/permissions")
public class PermissionOperations {

  private final AccessControlManager accessControlManager;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public PermissionOperations(AccessControlManager accessControlManager) {
    this.accessControlManager = accessControlManager;
  }

  @POST
  @Path("users/{user}/roles/")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "add-role-to-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "add-role-to-user", absolute = true)
  public Response addRoleToUser(
      @PathParam("metalake") String metalake,
      @PathParam("user") String user,
      RoleGrantRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new GrantResponse(
                      accessControlManager.addRoleToUser(metalake, request.getRoleName(), user))));
    } catch (Exception e) {
      return ExceptionHandlers.handleGrantException(
          OperationType.REMOVE, request.getRoleName(), user, e);
    }
  }

  @POST
  @Path("groups/{group}/roles/")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "add-role-to-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "add-role-to-group", absolute = true)
  public Response addRoleToGroup(
      @PathParam("metalake") String metalake,
      @PathParam("group") String group,
      RoleGrantRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new GrantResponse(
                      accessControlManager.addRoleToGroup(
                          metalake, request.getRoleName(), group))));
    } catch (Exception e) {
      return ExceptionHandlers.handleGrantException(
          OperationType.REMOVE, request.getRoleName(), group, e);
    }
  }

  @DELETE
  @Path("users/{user}/roles/{role}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "remove-role-from-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "remove-role-from-user", absolute = true)
  public Response removeRoleFromUser(
      @PathParam("metalake") String metalake,
      @PathParam("user") String user,
      @PathParam("role") String role) {
    try {
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new RemoveResponse(
                      accessControlManager.removeRoleFromUser(metalake, role, user))));
    } catch (Exception e) {
      return ExceptionHandlers.handleGrantException(OperationType.REMOVE, role, user, e);
    }
  }

  @DELETE
  @Path("groups/{group}/roles/{role}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "remove-role-from-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "remove-role-from-group", absolute = true)
  public Response removeRoleFrom(
      @PathParam("metalake") String metalake,
      @PathParam("group") String group,
      @PathParam("role") String role) {
    try {
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new RemoveResponse(
                      accessControlManager.removeRoleFromGroup(metalake, role, group))));
    } catch (Exception e) {
      return ExceptionHandlers.handleGrantException(OperationType.REMOVE, role, group, e);
    }
  }
}
