/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.Metalake;
import com.datastrato.gravitino.MetalakeChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.dto.MetalakeDTO;
import com.datastrato.gravitino.dto.requests.MetalakeCreateRequest;
import com.datastrato.gravitino.dto.requests.MetalakeUpdateRequest;
import com.datastrato.gravitino.dto.requests.MetalakeUpdatesRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.MetalakeListResponse;
import com.datastrato.gravitino.dto.responses.MetalakeResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.lock.LockType;
import com.datastrato.gravitino.lock.TreeLockUtils;
import com.datastrato.gravitino.metalake.MetalakeDispatcher;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.server.web.Utils;
import com.datastrato.gravitino.utils.NameIdentifierUtil;
import java.util.Arrays;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/metalakes")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class MetalakeOperations {

  private static final Logger LOG = LoggerFactory.getLogger(MetalakeOperations.class);

  private final MetalakeDispatcher metalakeDispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public MetalakeOperations(MetalakeDispatcher dispatcher) {
    this.metalakeDispatcher = dispatcher;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-metalake." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-metalake", absolute = true)
  public Response listMetalakes() {
    LOG.info("Received list metalakes request.");
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            Metalake[] metalakes =
                TreeLockUtils.doWithRootTreeLock(LockType.READ, metalakeDispatcher::listMetalakes);
            MetalakeDTO[] metalakeDTOS =
                Arrays.stream(metalakes).map(DTOConverters::toDTO).toArray(MetalakeDTO[]::new);
            Response response = Utils.ok(new MetalakeListResponse(metalakeDTOS));
            LOG.info("List {} metalakes in Gravitino", metalakeDTOS.length);
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleMetalakeException(
          OperationType.LIST, Namespace.empty().toString(), e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "create-metalake." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-metalake", absolute = true)
  public Response createMetalake(MetalakeCreateRequest request) {
    LOG.info("Received create metalake request for {}", request.getName());
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident = NameIdentifierUtil.ofMetalake(request.getName());
            Metalake metalake =
                TreeLockUtils.doWithRootTreeLock(
                    LockType.WRITE,
                    () ->
                        metalakeDispatcher.createMetalake(
                            ident, request.getComment(), request.getProperties()));
            Response response = Utils.ok(new MetalakeResponse(DTOConverters.toDTO(metalake)));
            LOG.info("Metalake created: {}", metalake.name());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleMetalakeException(OperationType.CREATE, request.getName(), e);
    }
  }

  @GET
  @Path("{name}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "load-metalake." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-metalake", absolute = true)
  public Response loadMetalake(@PathParam("name") String metalakeName) {
    LOG.info("Received load metalake request for metalake: {}", metalakeName);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier identifier = NameIdentifierUtil.ofMetalake(metalakeName);
            Metalake metalake =
                TreeLockUtils.doWithTreeLock(
                    identifier, LockType.READ, () -> metalakeDispatcher.loadMetalake(identifier));
            Response response = Utils.ok(new MetalakeResponse(DTOConverters.toDTO(metalake)));
            LOG.info("Metalake loaded: {}", metalake.name());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleMetalakeException(OperationType.LOAD, metalakeName, e);
    }
  }

  @PUT
  @Path("{name}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "alter-metalake." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-metalake", absolute = true)
  public Response alterMetalake(
      @PathParam("name") String metalakeName, MetalakeUpdatesRequest updatesRequest) {
    LOG.info("Received alter metalake request for metalake: {}", metalakeName);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            updatesRequest.validate();
            NameIdentifier identifier = NameIdentifierUtil.ofMetalake(metalakeName);
            MetalakeChange[] changes =
                updatesRequest.getUpdates().stream()
                    .map(MetalakeUpdateRequest::metalakeChange)
                    .toArray(MetalakeChange[]::new);
            Metalake updatedMetalake =
                TreeLockUtils.doWithRootTreeLock(
                    LockType.WRITE, () -> metalakeDispatcher.alterMetalake(identifier, changes));
            Response response =
                Utils.ok(new MetalakeResponse(DTOConverters.toDTO(updatedMetalake)));
            LOG.info("Metalake altered: {}", updatedMetalake.name());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleMetalakeException(OperationType.ALTER, metalakeName, e);
    }
  }

  @DELETE
  @Path("{name}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-metalake." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-metalake", absolute = true)
  public Response dropMetalake(@PathParam("name") String metalakeName) {
    LOG.info("Received drop metalake request for metalake: {}", metalakeName);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier identifier = NameIdentifierUtil.ofMetalake(metalakeName);
            boolean dropped =
                TreeLockUtils.doWithRootTreeLock(
                    LockType.WRITE, () -> metalakeDispatcher.dropMetalake(identifier));
            if (!dropped) {
              LOG.warn("Failed to drop metalake by name {}", metalakeName);
            }

            Response response = Utils.ok(new DropResponse(dropped));
            LOG.info("Metalake dropped: {}", metalakeName);
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleMetalakeException(OperationType.DROP, metalakeName, e);
    }
  }
}
