/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.authorization.AccessControlManager;
import com.datastrato.gravitino.dto.requests.UserAddRequest;
import com.datastrato.gravitino.dto.responses.RemoveResponse;
import com.datastrato.gravitino.dto.responses.UserResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.lock.LockType;
import com.datastrato.gravitino.lock.TreeLockUtils;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.server.web.Utils;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/admins")
public class MetalakeAdminOperations {
  private static final Logger LOG = LoggerFactory.getLogger(MetalakeAdminOperations.class);

  private final AccessControlManager accessControlManager;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public MetalakeAdminOperations(AccessControlManager accessControlManager) {
    this.accessControlManager = accessControlManager;
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "add-admin." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "add-admin", absolute = true)
  public Response addAdmin(UserAddRequest request) {

    try {
      NameIdentifier ident = ofMetalakeAdmin(request.getName());
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new UserResponse(
                      DTOConverters.toDTO(
                          TreeLockUtils.doWithTreeLock(
                              ident,
                              LockType.WRITE,
                              () -> accessControlManager.addMetalakeAdmin(request.getName()))))));
    } catch (Exception e) {
      return ExceptionHandlers.handleUserException(OperationType.ADD, request.getName(), null, e);
    }
  }

  @Path("{user}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "remove-admin." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "remove-admin", absolute = true)
  public Response removeAdmin(@PathParam("user") String user) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = ofMetalakeAdmin(user);
            boolean removed =
                TreeLockUtils.doWithTreeLock(
                    ident, LockType.WRITE, () -> accessControlManager.removeMetalakeAdmin(user));
            if (!removed) {
              LOG.warn("Failed to remove metalake admin user {}", user);
            }
            return Utils.ok(new RemoveResponse(removed));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleUserException(OperationType.REMOVE, user, null, e);
    }
  }

  private NameIdentifier ofMetalakeAdmin(String user) {
    return NameIdentifier.of(
        BaseMetalake.SYSTEM_METALAKE_RESERVED_NAME,
        CatalogEntity.AUTHORIZATION_CATALOG_NAME,
        SchemaEntity.ADMIN_SCHEMA_NAME,
        user);
  }
}
