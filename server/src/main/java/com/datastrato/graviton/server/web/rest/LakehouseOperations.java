package com.datastrato.graviton.server.web.rest;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.meta.*;
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

  private final BaseLakehouseOperations ops;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public LakehouseOperations(BaseLakehouseOperations ops) {
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

    LakehouseCreate create =
        LakehouseCreate.create(
            request.getName(),
            request.getComment(),
            request.getProperties(),
            Utils.remoteUser(httpRequest));
    NameIdentifier ident = NameIdentifier.parse(request.getName());

    try {
      Lakehouse lakehouse = ops.createEntity(ident, create);
      return Utils.ok(new LakehouseResponse(lakehouse));

    } catch (Exception e) {
      LOG.error("Failed to create lakehouse", e);

      return Utils.internalError(e.getMessage());
    }
  }

  // TODO. Are we going to use id or name to get Entity? @Jerry
  @GET
  @Path("{name}")
  @Produces("application/vnd.graviton.v1+json")
  public Response get(@PathParam("name") String lakehouseName) {
    if (lakehouseName == null || lakehouseName.isEmpty()) {
      return Utils.illegalArguments("Tenant name is required");
    }

    NameIdentifier identifier = NameIdentifier.parse(lakehouseName);
    try {
      Lakehouse lakehouse = ops.loadEntity(identifier);
      if (lakehouse == null) {
        LOG.warn("Failed to find lakehouse by name {}", lakehouseName);
        return Utils.notFound("Failed to find lakehouse by name " + lakehouseName);
      } else {
        return Utils.ok(new LakehouseResponse(lakehouse));
      }

    } catch (Exception e) {
      LOG.error("Failed to get lakehouse by name {}", lakehouseName, e);

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

    NameIdentifier identifier = NameIdentifier.parse(lakehouseName);
    try {
      boolean dropped = ops.dropEntity(identifier);
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
