/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.dto.requests.GroupCreateRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.GroupResponse;
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
  @Timed(name = "load-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-group", absolute = true)
  public Response loadGroup(
      @PathParam("metalake") String metalake, @PathParam("group") String group) {
    try {
      NameIdentifier ident = NameIdentifier.of(metalake, group);
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new GroupResponse(
                      DTOConverters.toDTO(
                          TreeLockUtils.doWithTreeLock(
                              ident,
                              LockType.READ,
                              () -> accessControlManager.loadGroup(metalake, group))))));
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupException(OperationType.LOAD, group, metalake, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "create-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-group", absolute = true)
  public Response createGroup(@PathParam("metalake") String metalake, GroupCreateRequest request) {
    try {
      NameIdentifier ident = NameIdentifier.of(metalake, request.getName());
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new GroupResponse(
                      DTOConverters.toDTO(
                          TreeLockUtils.doWithTreeLock(
                              ident,
                              LockType.WRITE,
                              () ->
                                  accessControlManager.createGroup(
                                      metalake,
                                      request.getName(),
                                      request.getUsers(),
                                      request.getProperties()))))));
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupException(
          OperationType.CREATE, request.getName(), metalake, e);
    }
  }

  @DELETE
  @Path("{group}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-group", absolute = true)
  public Response dropGroup(
      @PathParam("metalake") String metalake, @PathParam("group") String group) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifier.of(metalake, group);
            boolean dropped =
                TreeLockUtils.doWithTreeLock(
                    ident, LockType.WRITE, () -> accessControlManager.dropGroup(metalake, group));
            if (!dropped) {
              LOG.warn("Failed to drop table {} under metalake {}", group, metalake);
            }
            return Utils.ok(new DropResponse(dropped));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupException(OperationType.DROP, group, metalake, e);
    }
  }
}
