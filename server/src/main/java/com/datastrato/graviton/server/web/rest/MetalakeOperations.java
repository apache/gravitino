/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server.web.rest;

import com.datastrato.graviton.MetalakeChange;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.dto.MetalakeDTO;
import com.datastrato.graviton.dto.requests.MetalakeCreateRequest;
import com.datastrato.graviton.dto.requests.MetalakeUpdateRequest;
import com.datastrato.graviton.dto.requests.MetalakeUpdatesRequest;
import com.datastrato.graviton.dto.responses.DropResponse;
import com.datastrato.graviton.dto.responses.MetalakeListResponse;
import com.datastrato.graviton.dto.responses.MetalakeResponse;
import com.datastrato.graviton.dto.util.DTOConverters;
import com.datastrato.graviton.meta.BaseMetalake;
import com.datastrato.graviton.meta.MetalakeManager;
import com.datastrato.graviton.server.web.Utils;
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

  private final MetalakeManager manager;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public MetalakeOperations(MetalakeManager manager) {
    this.manager = manager;
  }

  @GET
  @Produces("application/vnd.graviton.v1+json")
  public Response listMetalakes() {
    try {
      BaseMetalake[] metalakes = manager.listMetalakes();
      MetalakeDTO[] metalakeDTOS =
          Arrays.stream(metalakes).map(DTOConverters::fromDTO).toArray(MetalakeDTO[]::new);
      return Utils.ok(new MetalakeListResponse(metalakeDTOS));

    } catch (Exception e) {
      return ExceptionHandlers.handleMetalakeException(
          OperationType.LIST, Namespace.empty().toString(), e);
    }
  }

  @POST
  @Produces("application/vnd.graviton.v1+json")
  public Response createMetalake(MetalakeCreateRequest request) {
    try {
      request.validate();
      NameIdentifier ident = NameIdentifier.ofMetalake(request.getName());
      BaseMetalake metalake =
          manager.createMetalake(ident, request.getComment(), request.getProperties());
      return Utils.ok(new MetalakeResponse(DTOConverters.fromDTO(metalake)));

    } catch (Exception e) {
      return ExceptionHandlers.handleMetalakeException(OperationType.CREATE, request.getName(), e);
    }
  }

  @GET
  @Path("{name}")
  @Produces("application/vnd.graviton.v1+json")
  public Response loadMetalake(@PathParam("name") String metalakeName) {
    try {
      NameIdentifier identifier = NameIdentifier.ofMetalake(metalakeName);
      BaseMetalake metalake = manager.loadMetalake(identifier);
      return Utils.ok(new MetalakeResponse(DTOConverters.fromDTO(metalake)));

    } catch (Exception e) {
      return ExceptionHandlers.handleMetalakeException(OperationType.LOAD, metalakeName, e);
    }
  }

  @PUT
  @Path("{name}")
  @Produces("application/vnd.graviton.v1+json")
  public Response alterMetalake(
      @PathParam("name") String metalakeName, MetalakeUpdatesRequest updatesRequest) {
    try {
      updatesRequest.validate();
      NameIdentifier identifier = NameIdentifier.ofMetalake(metalakeName);
      MetalakeChange[] changes =
          updatesRequest.getUpdates().stream()
              .map(MetalakeUpdateRequest::metalakeChange)
              .toArray(MetalakeChange[]::new);

      BaseMetalake updatedMetalake = manager.alterMetalake(identifier, changes);
      return Utils.ok(new MetalakeResponse(DTOConverters.fromDTO(updatedMetalake)));

    } catch (Exception e) {
      return ExceptionHandlers.handleMetalakeException(OperationType.ALTER, metalakeName, e);
    }
  }

  @DELETE
  @Path("{name}")
  @Produces("application/vnd.graviton.v1+json")
  public Response dropMetalake(@PathParam("name") String metalakeName) {
    try {
      NameIdentifier identifier = NameIdentifier.ofMetalake(metalakeName);
      boolean dropped = manager.dropMetalake(identifier);
      if (!dropped) {
        LOG.warn("Failed to drop metalake by name {}", metalakeName);
      }

      return Utils.ok(new DropResponse(dropped));

    } catch (Exception e) {
      return ExceptionHandlers.handleMetalakeException(OperationType.DROP, metalakeName, e);
    }
  }
}
