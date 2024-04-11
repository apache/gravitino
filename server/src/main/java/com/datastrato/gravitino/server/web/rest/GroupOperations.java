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
import com.datastrato.gravitino.dto.requests.GroupAddRequest;
import com.datastrato.gravitino.dto.responses.GroupResponse;
import com.datastrato.gravitino.dto.responses.RemoveResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.exceptions.ForbiddenException;
import com.datastrato.gravitino.lock.LockType;
import com.datastrato.gravitino.lock.TreeLockUtils;
import com.datastrato.gravitino.metalake.MetalakeManager;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.server.authorization.NameBindings;
import com.datastrato.gravitino.server.web.Utils;
import com.datastrato.gravitino.utils.PrincipalUtils;
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

@NameBindings.AccessControlInterfaces
@Path("/metalakes/{metalake}/groups")
public class GroupOperations {

  private static final Logger LOG = LoggerFactory.getLogger(GroupOperations.class);

  private final AccessControlManager accessControlManager;
  private final MetalakeManager metalakeManager;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public GroupOperations(MetalakeManager metalakeManager) {
    // Because accessManager may be null when Gravitino doesn't enable authorization,
    // and Jersey injection doesn't support null value. So GroupOperations chooses to retrieve
    // accessControlManager from GravitinoEnv instead of injection here.
    this.accessControlManager = GravitinoEnv.getInstance().accessControlManager();
    this.metalakeManager = metalakeManager;
  }

  @GET
  @Path("{group}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-group", absolute = true)
  public Response getGroup(
      @PathParam("metalake") String metalake, @PathParam("group") String group) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier identifier = NameIdentifier.ofMetalake(metalake);
            return TreeLockUtils.doWithTreeLock(
                identifier,
                LockType.READ,
                () ->
                    AuthorizationUtils.doWithLock(
                        () -> {
                          checkPermission(identifier);

                          return Utils.ok(
                              new GroupResponse(
                                  DTOConverters.toDTO(
                                      accessControlManager.getGroup(metalake, group))));
                        }));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupException(OperationType.GET, group, metalake, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "add-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "add-group", absolute = true)
  public Response addGroup(@PathParam("metalake") String metalake, GroupAddRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier identifier = NameIdentifier.ofMetalake(metalake);
            return TreeLockUtils.doWithTreeLock(
                identifier,
                LockType.READ,
                () ->
                    AuthorizationUtils.doWithLock(
                        () -> {
                          checkPermission(identifier);

                          return Utils.ok(
                              new GroupResponse(
                                  DTOConverters.toDTO(
                                      accessControlManager.addGroup(metalake, request.getName()))));
                        }));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleGroupException(
          OperationType.ADD, request.getName(), metalake, e);
    }
  }

  @DELETE
  @Path("{group}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "remove-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "remove-group", absolute = true)
  public Response removeGroup(
      @PathParam("metalake") String metalake, @PathParam("group") String group) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier identifier = NameIdentifier.ofMetalake(metalake);
            return TreeLockUtils.doWithTreeLock(
                identifier,
                LockType.READ,
                () ->
                    AuthorizationUtils.doWithLock(
                        () -> {
                          checkPermission(identifier);

                          boolean removed = accessControlManager.removeGroup(metalake, group);
                          if (!removed) {
                            LOG.warn(
                                "Failed to remove group {} under metalake {}", group, metalake);
                          }
                          return Utils.ok(new RemoveResponse(removed));
                        }));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupException(OperationType.REMOVE, group, metalake, e);
    }
  }

  // Only metalake admins can manage access control of his metalake.
  // Gravitino prefers adopting a cautious style not to expose `getUser` to non-admin users.
  private void checkPermission(NameIdentifier identifier) {
    String currentUser = PrincipalUtils.getCurrentPrincipal().getName();
    if (!accessControlManager.isMetalakeAdmin(currentUser)) {
      throw new ForbiddenException(
          "%s is not a metalake admin, only metalake admins can manage groups", currentUser);
    }

    String creator = metalakeManager.loadMetalake(identifier).auditInfo().creator();
    if (!currentUser.equals(creator)) {
      throw new ForbiddenException(
          "%s is not the creator of the metalake,"
              + "only the creator can manage groups of the metalake",
          currentUser);
    }
  }
}
