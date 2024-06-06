/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.authorization.AccessControlManager;
import com.datastrato.gravitino.authorization.AuthorizationUtils;
import com.datastrato.gravitino.dto.requests.RoleGrantRequest;
import com.datastrato.gravitino.dto.requests.RoleRevokeRequest;
import com.datastrato.gravitino.dto.responses.GroupResponse;
import com.datastrato.gravitino.dto.responses.UserResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.lock.LockType;
import com.datastrato.gravitino.lock.TreeLockUtils;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.server.authorization.NameBindings;
import com.datastrato.gravitino.server.web.Utils;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;

@NameBindings.AccessControlInterfaces
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

  @PUT
  @Path("users/{user}/grant/")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "grant-roles-to-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "grant-roles-to-user", absolute = true)
  public Response grantRolesToUser(
      @PathParam("metalake") String metalake,
      @PathParam("user") String user,
      RoleGrantRequest request) {
    try {
      TreeLockUtils.doWithTreeLock(
          NameIdentifier.of(metalake),
          LockType.READ,
          () -> AuthorizationUtils.checkMetalakeExists(metalake));

      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new UserResponse(
                      DTOConverters.toDTO(
                          accessControlManager.grantRolesToUser(
                              metalake, request.getRoleNames(), user)))));
    } catch (Exception e) {
      return ExceptionHandlers.handleUserPermissionOperationException(
          OperationType.GRANT, StringUtils.join(request.getRoleNames(), ","), user, e);
    }
  }

  @PUT
  @Path("groups/{group}/grant/")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "grant-roles-to-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "grant-roles-to-group", absolute = true)
  public Response grantRolesToGroup(
      @PathParam("metalake") String metalake,
      @PathParam("group") String group,
      RoleGrantRequest request) {
    try {
      TreeLockUtils.doWithTreeLock(
          NameIdentifier.of(metalake),
          LockType.READ,
          () -> AuthorizationUtils.checkMetalakeExists(metalake));

      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new GroupResponse(
                      DTOConverters.toDTO(
                          accessControlManager.grantRolesToGroup(
                              metalake, request.getRoleNames(), group)))));
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupPermissionOperationException(
          OperationType.GRANT, StringUtils.join(request.getRoleNames(), ","), group, e);
    }
  }

  @PUT
  @Path("users/{user}/revoke/")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "revoke-roles-from-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "revoke-roles-from-user", absolute = true)
  public Response revokeRolesFromUser(
      @PathParam("metalake") String metalake,
      @PathParam("user") String user,
      RoleRevokeRequest request) {
    try {
      TreeLockUtils.doWithTreeLock(
          NameIdentifier.of(metalake),
          LockType.READ,
          () -> AuthorizationUtils.checkMetalakeExists(metalake));

      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new UserResponse(
                      DTOConverters.toDTO(
                          accessControlManager.revokeRolesFromUser(
                              metalake, request.getRoleNames(), user)))));
    } catch (Exception e) {
      return ExceptionHandlers.handleUserPermissionOperationException(
          OperationType.REVOKE, StringUtils.join(request.getRoleNames(), ","), user, e);
    }
  }

  @PUT
  @Path("groups/{group}/revoke")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "revoke-roles-from-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "revokes-role-from-group", absolute = true)
  public Response revokeRolesFromGroup(
      @PathParam("metalake") String metalake,
      @PathParam("group") String group,
      RoleRevokeRequest request) {
    try {
      TreeLockUtils.doWithTreeLock(
          NameIdentifier.of(metalake),
          LockType.READ,
          () -> AuthorizationUtils.checkMetalakeExists(metalake));

      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new GroupResponse(
                      DTOConverters.toDTO(
                          accessControlManager.revokeRolesFromGroup(
                              metalake, request.getRoleNames(), group)))));
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupPermissionOperationException(
          OperationType.REVOKE, StringUtils.join(request.getRoleNames()), group, e);
    }
  }
}
