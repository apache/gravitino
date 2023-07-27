/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server.web.rest;

import com.datastrato.graviton.MetalakeChange;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.dto.MetalakeDTO;
import com.datastrato.graviton.dto.requests.MetalakeCreateRequest;
import com.datastrato.graviton.dto.requests.MetalakeUpdateRequest;
import com.datastrato.graviton.dto.requests.MetalakeUpdatesRequest;
import com.datastrato.graviton.dto.responses.DropResponse;
import com.datastrato.graviton.dto.responses.MetalakeListResponse;
import com.datastrato.graviton.dto.responses.MetalakeResponse;
import com.datastrato.graviton.exceptions.MetalakeAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.datastrato.graviton.meta.BaseMetalake;
import com.datastrato.graviton.meta.BaseMetalakesOperations;
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

  private final BaseMetalakesOperations ops;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public MetalakeOperations(BaseMetalakesOperations ops) {
    this.ops = ops;
  }

  @GET
  @Produces("application/vnd.graviton.v1+json")
  public Response listMetalakes() {
    try {
      BaseMetalake[] metalakes = ops.listMetalakes();
      MetalakeDTO[] metalakeDTOS =
          Arrays.stream(metalakes).map(DTOConverters::toDTO).toArray(MetalakeDTO[]::new);
      return Utils.ok(new MetalakeListResponse(metalakeDTOS));

    } catch (Exception e) {
      LOG.error("Failed to list metalakes", e);
      return Utils.internalError("Failed to list metalake", e);
    }
  }

  @POST
  @Produces("application/vnd.graviton.v1+json")
  public Response createMetalake(MetalakeCreateRequest request) {
    try {
      request.validate();
    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate MetalakeCreateRequest arguments {}", request, e);

      return Utils.illegalArguments(
          "Failed to validate MetalakeCreateRequest arguments " + request, e);
    }

    try {
      NameIdentifier ident = NameIdentifier.parse(request.getName());
      BaseMetalake metalake =
          ops.createMetalake(ident, request.getComment(), request.getProperties());
      return Utils.ok(new MetalakeResponse(DTOConverters.toDTO(metalake)));

    } catch (MetalakeAlreadyExistsException exception) {
      LOG.warn("Metalake {} already exists", request.getName(), exception);
      return Utils.alreadyExists("Metalake " + request.getName() + " already exists", exception);

    } catch (Exception e) {
      LOG.error("Failed to create metalake", e);
      return Utils.internalError("Failed to create metalake", e);
    }
  }

  @GET
  @Path("{name}")
  @Produces("application/vnd.graviton.v1+json")
  public Response loadMetalake(@PathParam("name") String metalakeName) {
    if (metalakeName == null || metalakeName.isEmpty()) {
      LOG.error("Metalake name is null or empty");
      return Utils.illegalArguments("Metalake name is illegal");
    }

    try {
      NameIdentifier identifier = NameIdentifier.parse(metalakeName);
      BaseMetalake metalake = ops.loadMetalake(identifier);
      return Utils.ok(new MetalakeResponse(DTOConverters.toDTO(metalake)));

    } catch (NoSuchMetalakeException e) {
      LOG.warn("Metalake {} does not exist", metalakeName);
      return Utils.notFound("Metalake " + metalakeName + " does not exist", e);

    } catch (Exception e) {
      LOG.error("Failed to load metalake {}", metalakeName, e);
      return Utils.internalError("Failed to load metalake " + metalakeName, e);
    }
  }

  @PUT
  @Path("{name}")
  @Produces("application/vnd.graviton.v1+json")
  public Response alterMetalake(
      @PathParam("name") String metalakeName, MetalakeUpdatesRequest updatesRequest) {
    if (metalakeName == null || metalakeName.isEmpty()) {
      LOG.error("Metalake name is null or empty");
      return Utils.illegalArguments("Metalake name is illegal");
    }

    try {
      updatesRequest.validate();
    } catch (Exception e) {
      LOG.error("Failed to validate MetalakeUpdatesRequest arguments {}", updatesRequest, e);
      return Utils.illegalArguments(
          "Failed to validate MetalakeUpdatesRequest arguments " + updatesRequest, e);
    }

    try {
      NameIdentifier identifier = NameIdentifier.parse(metalakeName);
      MetalakeChange[] changes =
          updatesRequest.getRequests().stream()
              .map(MetalakeUpdateRequest::metalakeChange)
              .toArray(MetalakeChange[]::new);

      BaseMetalake updatedMetalake = ops.alterMetalake(identifier, changes);
      return Utils.ok(new MetalakeResponse(DTOConverters.toDTO(updatedMetalake)));

    } catch (NoSuchMetalakeException e) {
      LOG.warn("Metalake {} does not exist", metalakeName);
      return Utils.notFound("Metalake " + metalakeName + " does not exist", e);

    } catch (IllegalArgumentException ex) {
      LOG.error("Failed to alter metalake {} by unsupported change", metalakeName, ex);
      return Utils.illegalArguments(
          "Failed to alter metalake " + metalakeName + " by unsupported change", ex);

    } catch (Exception e) {
      LOG.error("Failed to update metalake {}", metalakeName, e);
      return Utils.internalError("Failed to update metalake " + metalakeName, e);
    }
  }

  @DELETE
  @Path("{name}")
  @Produces("application/vnd.graviton.v1+json")
  public Response dropMetalake(@PathParam("name") String metalakeName) {
    if (metalakeName == null || metalakeName.isEmpty()) {
      LOG.error("Metalake name is null or empty");
      return Utils.illegalArguments("Metalake name is illegal");
    }

    try {
      NameIdentifier identifier = NameIdentifier.parse(metalakeName);
      boolean dropped = ops.dropMetalake(identifier);
      if (!dropped) {
        LOG.warn("Failed to drop metalake by name {}", metalakeName);
      }

      return Utils.ok(new DropResponse(dropped));

    } catch (Exception e) {
      LOG.error("Failed to drop metalake {}", metalakeName, e);
      return Utils.internalError("Failed to drop metalake " + metalakeName, e);
    }
  }
}
