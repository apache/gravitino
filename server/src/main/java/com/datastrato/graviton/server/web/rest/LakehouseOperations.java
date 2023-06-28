package com.datastrato.graviton.server.web.rest;

import com.datastrato.graviton.LakehouseChange;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.dto.requests.LakehouseCreateRequest;
import com.datastrato.graviton.dto.requests.LakehouseUpdateRequest;
import com.datastrato.graviton.dto.requests.LakehouseUpdatesRequest;
import com.datastrato.graviton.dto.responses.LakehouseResponse;
import com.datastrato.graviton.exceptions.NoSuchLakehouseException;
import com.datastrato.graviton.meta.BaseLakehouse;
import com.datastrato.graviton.meta.BaseLakehousesOperations;
import com.datastrato.graviton.server.web.Utils;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/lakehouses")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class LakehouseOperations {

  private static final Logger LOG = LoggerFactory.getLogger(LakehouseOperations.class);

  private final BaseLakehousesOperations ops;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public LakehouseOperations(BaseLakehousesOperations ops) {
    this.ops = ops;
  }

  @POST
  @Produces("application/vnd.graviton.v1+json")
  public Response create(LakehouseCreateRequest request) {
    try {
      request.validate();
    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate create Lakehouse arguments {}", request, e);

      return Utils.illegalArguments(e.getMessage());
    }

    try {
      NameIdentifier ident = NameIdentifier.parse(request.getName());
      BaseLakehouse lakehouse =
          ops.createLakehouse(ident, request.getComment(), request.getProperties());
      return Utils.ok(new LakehouseResponse(DTOConverters.toDTO(lakehouse)));

    } catch (Exception e) {
      LOG.error("Failed to create lakehouse", e);

      return Utils.internalError(e.getMessage());
    }
  }

  @GET
  @Path("{name}")
  @Produces("application/vnd.graviton.v1+json")
  public Response get(@PathParam("name") String lakehouseName) {
    if (lakehouseName == null || lakehouseName.isEmpty()) {
      return Utils.illegalArguments("Tenant name is required");
    }

    try {
      NameIdentifier identifier = NameIdentifier.parse(lakehouseName);
      BaseLakehouse lakehouse = ops.loadLakehouse(identifier);
      return Utils.ok(new LakehouseResponse(DTOConverters.toDTO(lakehouse)));

    } catch (NoSuchLakehouseException e) {
      LOG.warn("Failed to find lakehouse by name {}", lakehouseName);
      return Utils.notFound("Failed to find lakehouse by name " + lakehouseName);

    } catch (Exception e) {
      LOG.error("Failed to get lakehouse by name {}", lakehouseName, e);

      return Utils.internalError(e.getMessage());
    }
  }

  @PUT
  @Path("{name}")
  @Produces("application/vnd.graviton.v1+json")
  public Response update(
      @PathParam("name") String lakehouseName, LakehouseUpdatesRequest updatesRequest) {
    if (lakehouseName == null || lakehouseName.isEmpty()) {
      return Utils.illegalArguments("Lakehouse name is required");
    }

    try {
      updatesRequest.validate();
    } catch (Exception e) {
      LOG.error("Failed to validate update Lakehouse arguments {}", updatesRequest, e);
      return Utils.illegalArguments(e.getMessage());
    }

    try {
      NameIdentifier identifier = NameIdentifier.parse(lakehouseName);
      LakehouseChange[] changes =
          updatesRequest.getRequests().stream()
              .map(LakehouseUpdateRequest::lakehouseChange)
              .toArray(LakehouseChange[]::new);

      BaseLakehouse updatedLakehouse = ops.alterLakehouse(identifier, changes);
      return Utils.ok(new LakehouseResponse(DTOConverters.toDTO(updatedLakehouse)));

    } catch (NoSuchLakehouseException e) {
      LOG.warn("Failed to find lakehouse by name {}", lakehouseName);
      return Utils.notFound("Failed to find lakehouse by name " + lakehouseName);

    } catch (Exception e) {
      LOG.error("Failed to update lakehouse by name {}", lakehouseName, e);
      return Utils.internalError(e.getMessage());
    }
  }

  @DELETE
  @Path("{name}")
  @Produces("application/vnd.graviton.v1+json")
  public Response delete(@PathParam("name") String lakehouseName) {
    if (lakehouseName == null || lakehouseName.isEmpty()) {
      return Utils.illegalArguments("Lakehouse name is required");
    }

    try {
      NameIdentifier identifier = NameIdentifier.parse(lakehouseName);
      boolean dropped = ops.dropLakehouse(identifier);
      if (dropped) {
        return Utils.ok();
      } else {
        LOG.warn("Failed to delete lakehouse by name {}", lakehouseName);
        return Utils.internalError("Failed to delete lakehouse by name " + lakehouseName);
      }

    } catch (Exception e) {
      LOG.error("Failed to delete lakehouse by name {}", lakehouseName, e);
      return Utils.internalError(e.getMessage());
    }
  }
}
