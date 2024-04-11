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
import com.datastrato.gravitino.dto.requests.UserAddRequest;
import com.datastrato.gravitino.dto.responses.RemoveResponse;
import com.datastrato.gravitino.dto.responses.UserResponse;
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
@Path("/metalakes/{metalake}/users")
public class UserOperations {

  private static final Logger LOG = LoggerFactory.getLogger(UserOperations.class);

  private final AccessControlManager accessControlManager;
  private final MetalakeManager metalakeManager;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public UserOperations(MetalakeManager metalakeManager) {
    // Because accessManager may be null when Gravitino doesn't enable authorization,
    // and Jersey injection doesn't support null value. So UserOperations chooses to retrieve
    // accessControlManager from GravitinoEnv instead of injection here.
    this.accessControlManager = GravitinoEnv.getInstance().accessControlManager();
    this.metalakeManager = metalakeManager;
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
                              new UserResponse(
                                  DTOConverters.toDTO(
                                      accessControlManager.getUser(metalake, user))));
                        }));
          });
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
                              new UserResponse(
                                  DTOConverters.toDTO(
                                      accessControlManager.addUser(metalake, request.getName()))));
                        }));
          });
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
            NameIdentifier identifier = NameIdentifier.ofMetalake(metalake);
            return TreeLockUtils.doWithTreeLock(
                identifier,
                LockType.READ,
                () ->
                    AuthorizationUtils.doWithLock(
                        () -> {
                          checkPermission(identifier);

                          boolean removed = accessControlManager.removeUser(metalake, user);
                          if (!removed) {
                            LOG.warn("Failed to remove user {} under metalake {}", user, metalake);
                          }
                          return Utils.ok(new RemoveResponse(removed));
                        }));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleUserException(OperationType.REMOVE, user, metalake, e);
    }
  }

  // Only metalake admins can manage access control of his metalake.
  // Gravitino prefers adopting a cautious style not to expose `getUser` to non-admin users.
  private void checkPermission(NameIdentifier identifier) {
    String currentUser = PrincipalUtils.getCurrentPrincipal().getName();
    if (!accessControlManager.isMetalakeAdmin(currentUser)) {
      throw new ForbiddenException(
          "%s is not a metalake admin, only metalake admins can manage users", currentUser);
    }

    String creator = metalakeManager.loadMetalake(identifier).auditInfo().creator();
    if (!currentUser.equals(creator)) {
      throw new ForbiddenException(
          "%s is not the creator of the metalake,"
              + "only the creator can manage users of the metalake",
          currentUser);
    }
  }
}
