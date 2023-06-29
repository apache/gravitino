package com.datastrato.graviton.server.web.rest;

import com.datastrato.graviton.MetalakeChange;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.dto.requests.MetalakeCreateRequest;
import com.datastrato.graviton.dto.requests.MetalakeUpdateRequest;
import com.datastrato.graviton.dto.requests.MetalakeUpdatesRequest;
import com.datastrato.graviton.dto.responses.MetalakeResponse;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.datastrato.graviton.meta.BaseMetalake;
import com.datastrato.graviton.meta.BaseMetalakesOperations;
import com.datastrato.graviton.server.web.Utils;
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

  @POST
  @Produces("application/vnd.graviton.v1+json")
  public Response create(MetalakeCreateRequest request) {
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

    } catch (Exception e) {
      LOG.error("Failed to create metalake", e);

      return Utils.internalError(e.getMessage());
    }
  }

  @GET
  @Path("{name}")
  @Produces("application/vnd.graviton.v1+json")
  public Response get(@PathParam("name") String metalakeName) {
    if (metalakeName == null || metalakeName.isEmpty()) {
      return Utils.illegalArguments("Tenant name is required");
    }

    try {
      NameIdentifier identifier = NameIdentifier.parse(metalakeName);
      BaseMetalake metalake = ops.loadMetalake(identifier);
      return Utils.ok(new MetalakeResponse(DTOConverters.toDTO(metalake)));

    } catch (NoSuchMetalakeException e) {
      LOG.warn("Failed to find metalake by name {}", metalakeName);
      return Utils.notFound("Failed to find metalake by name " + metalakeName);

    } catch (Exception e) {
      LOG.error("Failed to get metalake by name {}", metalakeName, e);

      return Utils.internalError(e.getMessage());
    }
  }

  @PUT
  @Path("{name}")
  @Produces("application/vnd.graviton.v1+json")
  public Response update(
      @PathParam("name") String metalakeName, MetalakeUpdatesRequest updatesRequest) {
    if (metalakeName == null || metalakeName.isEmpty()) {
      return Utils.illegalArguments("Metalake name is required");
    }

    try {
      updatesRequest.validate();
    } catch (Exception e) {
      LOG.error("Failed to validate update metalake arguments {}", updatesRequest, e);
      return Utils.illegalArguments(e.getMessage());
    }

    try {
      NameIdentifier identifier = NameIdentifier.parse(metalakeName);
      MetalakeChange[] changes =
          updatesRequest.getRequests().stream()
              .map(MetalakeUpdateRequest::metalakeChange)
              .toArray(MetalakeChange[]::new);

      BaseMetalake updatedmetalake = ops.alterMetalake(identifier, changes);
      return Utils.ok(new MetalakeResponse(DTOConverters.toDTO(updatedmetalake)));

    } catch (NoSuchMetalakeException e) {
      LOG.warn("Failed to find metalake by name {}", metalakeName);
      return Utils.notFound("Failed to find metalake by name " + metalakeName);

    } catch (Exception e) {
      LOG.error("Failed to update metalake by name {}", metalakeName, e);
      return Utils.internalError(e.getMessage());
    }
  }

  @DELETE
  @Path("{name}")
  @Produces("application/vnd.graviton.v1+json")
  public Response delete(@PathParam("name") String metalakeName) {
    if (metalakeName == null || metalakeName.isEmpty()) {
      return Utils.illegalArguments("metalake name is required");
    }

    try {
      NameIdentifier identifier = NameIdentifier.parse(metalakeName);
      boolean dropped = ops.dropMetalake(identifier);
      if (dropped) {
        return Utils.ok();
      } else {
        LOG.warn("Failed to delete metalake by name {}", metalakeName);
        return Utils.internalError("Failed to delete metalake by name " + metalakeName);
      }

    } catch (Exception e) {
      LOG.error("Failed to delete metalake by name {}", metalakeName, e);
      return Utils.internalError(e.getMessage());
    }
  }
}
