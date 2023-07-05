package com.datastrato.graviton.server.web.rest;

import com.datastrato.graviton.MetalakeChange;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.dto.MetalakeDTO;
import com.datastrato.graviton.dto.requests.MetalakeCreateRequest;
import com.datastrato.graviton.dto.requests.MetalakeUpdateRequest;
import com.datastrato.graviton.dto.requests.MetalakeUpdatesRequest;
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
import javax.ws.rs.*;
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
      return Utils.internalError(e.getMessage());
    }
  }

  @POST
  @Produces("application/vnd.graviton.v1+json")
  public Response createMetalake(MetalakeCreateRequest request) {
    try {
      request.validate();
    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate create Metalake arguments {}", request, e);

      return Utils.illegalArguments(e.getMessage());
    }

    try {
      NameIdentifier ident = NameIdentifier.parse(request.getName());
      BaseMetalake metalake =
          ops.createMetalake(ident, request.getComment(), request.getProperties());
      return Utils.ok(new MetalakeResponse(DTOConverters.toDTO(metalake)));

    } catch (MetalakeAlreadyExistsException exception) {
      LOG.error("Metalake with name {} already exists", request.getName(), exception);
      return Utils.alreadyExists(exception.getMessage());

    } catch (Exception e) {
      LOG.error("Failed to create metalake", e);
      return Utils.internalError(e.getMessage());
    }
  }

  @GET
  @Path("{name}")
  @Produces("application/vnd.graviton.v1+json")
  public Response loadMetalake(@PathParam("name") String metalakeName) {
    if (metalakeName == null || metalakeName.isEmpty()) {
      LOG.error("Metalake name is null or empty");
      return Utils.illegalArguments("Metalake name is required");
    }

    try {
      NameIdentifier identifier = NameIdentifier.parse(metalakeName);
      BaseMetalake metalake = ops.loadMetalake(identifier);
      return Utils.ok(new MetalakeResponse(DTOConverters.toDTO(metalake)));

    } catch (NoSuchMetalakeException e) {
      LOG.error("Failed to find metalake by name {}", metalakeName);
      return Utils.notFound(e.getMessage());

    } catch (Exception e) {
      LOG.error("Failed to get metalake by name {}", metalakeName, e);
      return Utils.internalError(e.getMessage());
    }
  }

  @PUT
  @Path("{name}")
  @Produces("application/vnd.graviton.v1+json")
  public Response alterMetalake(
      @PathParam("name") String metalakeName, MetalakeUpdatesRequest updatesRequest) {
    if (metalakeName == null || metalakeName.isEmpty()) {
      LOG.error("Metalake name is null or empty");
      return Utils.illegalArguments("Metalake name is required");
    }

    try {
      updatesRequest.validate();
    } catch (Exception e) {
      LOG.error("Failed to validate alter metalake arguments {}", updatesRequest, e);
      return Utils.illegalArguments(e.getMessage());
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
      LOG.error("Failed to find metalake by name {}", metalakeName);
      return Utils.notFound(e.getMessage());

    } catch (IllegalArgumentException ex) {
      LOG.error("Failed to alter metalake by name {} by unsupported change", metalakeName, ex);
      return Utils.illegalArguments(ex.getMessage());

    } catch (Exception e) {
      LOG.error("Failed to update metalake by name {}", metalakeName, e);
      return Utils.internalError(e.getMessage());
    }
  }

  @DELETE
  @Path("{name}")
  @Produces("application/vnd.graviton.v1+json")
  public Response dropMetalake(@PathParam("name") String metalakeName) {
    if (metalakeName == null || metalakeName.isEmpty()) {
      LOG.error("Metalake name is null or empty");
      return Utils.illegalArguments("metalake name is required");
    }

    try {
      NameIdentifier identifier = NameIdentifier.parse(metalakeName);
      boolean dropped = ops.dropMetalake(identifier);
      if (dropped) {
        return Utils.ok();
      } else {
        LOG.warn("Failed to drop metalake by name {}", metalakeName);
        return Utils.internalError("Failed to drop metalake by name " + metalakeName);
      }

    } catch (Exception e) {
      LOG.error("Failed to drop metalake by name {}", metalakeName, e);
      return Utils.internalError(e.getMessage());
    }
  }
}
