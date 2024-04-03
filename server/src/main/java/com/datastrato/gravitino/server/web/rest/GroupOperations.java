/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.authorization.AccessControlManager;
import com.datastrato.gravitino.dto.requests.GroupAddRequest;
import com.datastrato.gravitino.dto.responses.GroupResponse;
import com.datastrato.gravitino.dto.responses.RemoveResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.lock.LockType;
import com.datastrato.gravitino.lock.TreeLockUtils;
import com.datastrato.gravitino.meta.EntitySpecificConstants;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.server.web.Utils;
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

@Path("/metalakes/{metalake}/groups")
public class GroupOperations {

  private static final Logger LOG = LoggerFactory.getLogger(GroupOperations.class);

  private final AccessControlManager accessControlManager;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public GroupOperations(AccessControlManager accessControlManager) {
    this.accessControlManager = accessControlManager;
  }

  @GET
  @Path("{group}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-group", absolute = true)
  public Response getGroup(
      @PathParam("metalake") String metalake, @PathParam("group") String group) {
    try {
      NameIdentifier ident = ofGroup(metalake, group);
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new GroupResponse(
                      DTOConverters.toDTO(
                          TreeLockUtils.doWithTreeLock(
                              ident,
                              LockType.READ,
                              () -> accessControlManager.getGroup(metalake, group))))));
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
      NameIdentifier ident = ofGroup(metalake, request.getName());
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new GroupResponse(
                      DTOConverters.toDTO(
                          TreeLockUtils.doWithTreeLock(
                              ident,
                              LockType.WRITE,
                              () -> accessControlManager.addGroup(metalake, request.getName()))))));
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
            NameIdentifier ident = ofGroup(metalake, group);
            boolean removed =
                TreeLockUtils.doWithTreeLock(
                    ident, LockType.WRITE, () -> accessControlManager.removeGroup(metalake, group));
            if (!removed) {
              LOG.warn("Failed to remove group {} under metalake {}", group, metalake);
            }
            return Utils.ok(new RemoveResponse(removed));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupException(OperationType.REMOVE, group, metalake, e);
    }
  }

  private NameIdentifier ofGroup(String metalake, String group) {
    return NameIdentifier.of(
        metalake,
        EntitySpecificConstants.SYSTEM_CATALOG_RESERVED_NAME,
        EntitySpecificConstants.GROUP_SCHEMA_NAME,
        group);
  }
}
