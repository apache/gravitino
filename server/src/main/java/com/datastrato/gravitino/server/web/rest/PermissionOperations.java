/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.authorization.AccessControlManager;
import com.datastrato.gravitino.dto.requests.RoleGrantRequest;
import com.datastrato.gravitino.dto.responses.GrantResponse;
import com.datastrato.gravitino.dto.responses.RevokeResponse;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.server.web.Utils;
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

  public PermissionOperations() {
    // Because accessManager may be null when Gravitino doesn't enable authorization,
    // and Jersey injection doesn't support null value. So PermissionOperations chooses to retrieve
    // accessControlManager from GravitinoEnv instead of injection here.
    this.accessControlManager = GravitinoEnv.getInstance().accessControlManager();
  }

  @POST
  @Path("users/{user}/roles/")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "grant-role-to-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "grant-role-to-user", absolute = true)
  public Response grantRoleToUser(
      @PathParam("metalake") String metalake,
      @PathParam("user") String user,
      RoleGrantRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new GrantResponse(
                      accessControlManager.grantRoleToUser(
                          metalake, request.getRoleName(), user))));
    } catch (Exception e) {
      return ExceptionHandlers.handleUserPermissionOperationException(
          OperationType.GRANT, request.getRoleName(), user, e);
    }
  }

  @POST
  @Path("groups/{group}/roles/")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "grant-role-to-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "grant-role-to-group", absolute = true)
  public Response grantRoleToGroup(
      @PathParam("metalake") String metalake,
      @PathParam("group") String group,
      RoleGrantRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new GrantResponse(
                      accessControlManager.grantRoleToGroup(
                          metalake, request.getRoleName(), group))));
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupPermissionOperationException(
          OperationType.GRANT, request.getRoleName(), group, e);
    }
  }

  @DELETE
  @Path("users/{user}/roles/{role}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "revoke-role-from-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "revoke-role-from-user", absolute = true)
  public Response revokeRoleFromUser(
      @PathParam("metalake") String metalake,
      @PathParam("user") String user,
      @PathParam("role") String role) {
    try {
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new RevokeResponse(
                      accessControlManager.revokeRoleFromUser(metalake, role, user))));
    } catch (Exception e) {
      return ExceptionHandlers.handleUserPermissionOperationException(
          OperationType.REVOKE, role, user, e);
    }
  }

  @DELETE
  @Path("groups/{group}/roles/{role}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "revoke-role-from-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "revoke-role-from-group", absolute = true)
  public Response revokeRoleFromGroup(
      @PathParam("metalake") String metalake,
      @PathParam("group") String group,
      @PathParam("role") String role) {
    try {
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new RevokeResponse(
                      accessControlManager.revokeRoleFromGroup(metalake, role, group))));
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupPermissionOperationException(
          OperationType.REVOKE, role, group, e);
    }
  }
}
