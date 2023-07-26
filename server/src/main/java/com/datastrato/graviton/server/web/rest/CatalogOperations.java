/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server.web.rest;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.CatalogChange;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.catalog.CatalogManager;
import com.datastrato.graviton.dto.requests.CatalogCreateRequest;
import com.datastrato.graviton.dto.requests.CatalogUpdateRequest;
import com.datastrato.graviton.dto.requests.CatalogUpdatesRequest;
import com.datastrato.graviton.dto.responses.CatalogListResponse;
import com.datastrato.graviton.dto.responses.CatalogResponse;
import com.datastrato.graviton.dto.responses.DropResponse;
import com.datastrato.graviton.exceptions.CatalogAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.datastrato.graviton.server.web.Utils;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/metalakes/{metalake}/catalogs")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class CatalogOperations {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogOperations.class);

  private final CatalogManager manager;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public CatalogOperations(CatalogManager manager) {
    this.manager = manager;
  }

  @GET
  @Produces("application/vnd.graviton.v1+json")
  public Response listCatalogs(@PathParam("metalake") String metalake) {
    if (metalake == null || metalake.isEmpty()) {
      LOG.error("Metalake name is null or empty");
      return Utils.illegalArguments("Metalake name is illegal");
    }

    try {
      Namespace metalakeNS = Namespace.of(metalake);
      NameIdentifier[] idents = manager.listCatalogs(metalakeNS);
      return Utils.ok(new CatalogListResponse(idents));

    } catch (NoSuchMetalakeException ex) {
      LOG.error("Metalake {} does not exist, fail to list catalogs", metalake);
      return Utils.notFound("Metalake " + metalake + " does not exist", ex);

    } catch (Exception e) {
      LOG.error("Failed to list catalogs under metalake {}", metalake, e);
      return Utils.internalError("Failed to list catalogs under metalake " + metalake, e);
    }
  }

  @POST
  @Produces("application/vnd.graviton.v1+json")
  public Response createCatalog(
      @PathParam("metalake") String metalake, CatalogCreateRequest request) {
    if (metalake == null || metalake.isEmpty()) {
      LOG.error("Metalake name is null or empty");
      return Utils.illegalArguments("Metalake name is illegal");
    }

    try {
      request.validate();
    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate CreateCatalogRequest arguments {}", request, e);
      return Utils.illegalArguments(
          "Failed to validate CreateCatalogRequest arguments " + request, e);
    }

    try {
      NameIdentifier ident = NameIdentifier.of(metalake, request.getName());
      Catalog catalog =
          manager.createCatalog(
              ident, request.getType(), request.getComment(), request.getProperties());
      return Utils.ok(new CatalogResponse(DTOConverters.toDTO(catalog)));

    } catch (NoSuchMetalakeException ex) {
      LOG.error("Metalake {} does not exist, fail to create catalog", metalake);
      return Utils.notFound("Metalake " + metalake + " does not exist", ex);

    } catch (CatalogAlreadyExistsException ex) {
      LOG.error("Catalog {} already exists under metalake {}", request.getName(), metalake);
      return Utils.alreadyExists(
          String.format("Catalog %s already exists under metalake %s", request.getName(), metalake),
          ex);

    } catch (Exception e) {
      LOG.error("Failed to create catalog under metalake {}", metalake, e);
      return Utils.internalError("Failed to create catalog under metalake " + metalake, e);
    }
  }

  @GET
  @Path("{catalog}")
  @Produces("application/vnd.graviton.v1+json")
  public Response loadCatalog(
      @PathParam("metalake") String metalakeName, @PathParam("catalog") String catalogName) {
    if (metalakeName == null || metalakeName.isEmpty()) {
      LOG.error("Metalake name is null or empty");
      return Utils.illegalArguments("Metalake name is illegal");
    }

    if (catalogName == null || catalogName.isEmpty()) {
      LOG.error("Catalog name is null or empty");
      return Utils.illegalArguments("Catalog name is illegal");
    }

    try {
      NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName);
      Catalog catalog = manager.loadCatalog(ident);
      return Utils.ok(new CatalogResponse(DTOConverters.toDTO(catalog)));

    } catch (NoSuchMetalakeException ex) {
      LOG.error("Metalake {} does not exist, fail to load catalog {}", metalakeName, catalogName);
      return Utils.notFound("Metalake " + metalakeName + " does not exist", ex);

    } catch (NoSuchCatalogException ex) {
      LOG.error("Catalog {} does not exist under metalake {}", catalogName, metalakeName);
      return Utils.notFound(
          String.format("Catalog %s does not exist under metalake %s", catalogName, metalakeName),
          ex);

    } catch (Exception e) {
      LOG.error("Failed to load catalog {} under metalake {}", catalogName, metalakeName, e);
      return Utils.internalError(
          "Failed to load catalog " + catalogName + " under metalake " + metalakeName, e);
    }
  }

  @PUT
  @Path("{catalog}")
  @Produces("application/vnd.graviton.v1+json")
  public Response alterCatalog(
      @PathParam("metalake") String metalakeName,
      @PathParam("catalog") String catalogName,
      CatalogUpdatesRequest request) {
    if (metalakeName == null || metalakeName.isEmpty()) {
      LOG.error("Metalake name is null or empty");
      return Utils.illegalArguments("Metalake name is illegal");
    }

    if (catalogName == null || catalogName.isEmpty()) {
      LOG.error("Catalog name is null or empty");
      return Utils.illegalArguments("Catalog name is illegal");
    }

    try {
      request.validate();
    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate CatalogUpdatesRequest arguments {}", request, e);
      return Utils.illegalArguments(
          "Failed to validate CatalogUpdatesRequest arguments " + request, e);
    }

    try {
      NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName);
      CatalogChange[] changes =
          request.getRequests().stream()
              .map(CatalogUpdateRequest::catalogChange)
              .toArray(CatalogChange[]::new);

      Catalog catalog = manager.alterCatalog(ident, changes);
      return Utils.ok(new CatalogResponse(DTOConverters.toDTO(catalog)));

    } catch (NoSuchCatalogException ex) {
      LOG.error("Catalog {} does not exist under metalake {}", catalogName, metalakeName);
      return Utils.notFound(
          "Catalog " + catalogName + " does not exist under metalake " + metalakeName, ex);

    } catch (IllegalArgumentException ex) {
      LOG.error(
          "Failed to alter catalog {} under metalake {} with unsupported changes",
          catalogName,
          metalakeName,
          ex);
      return Utils.illegalArguments(
          "Failed to alter catalog "
              + catalogName
              + " under metalake "
              + metalakeName
              + " with unsupported changes",
          ex);

    } catch (Exception e) {
      LOG.error("Failed to alter catalog {} under metalake {}", catalogName, metalakeName, e);
      return Utils.internalError(
          "Failed to alter catalog " + catalogName + " under metalake " + metalakeName, e);
    }
  }

  @DELETE
  @Path("{catalog}")
  @Produces("application/vnd.graviton.v1+json")
  public Response dropCatalog(
      @PathParam("metalake") String metalakeName, @PathParam("catalog") String catalogName) {
    if (metalakeName == null || metalakeName.isEmpty()) {
      LOG.error("Metalake name is null or empty");
      return Utils.illegalArguments("Metalake name is illegal");
    }

    if (catalogName == null || catalogName.isEmpty()) {
      LOG.error("Catalog name is null or empty");
      return Utils.illegalArguments("Catalog name is illegal");
    }

    try {
      NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName);
      boolean dropped = manager.dropCatalog(ident);
      if (!dropped) {
        LOG.warn("Failed to drop catalog {} under metalake {}", catalogName, metalakeName);
      }

      return Utils.ok(new DropResponse(dropped));

    } catch (Exception e) {
      LOG.error("Failed to drop catalog {} under metalake {}", catalogName, metalakeName, e);
      return Utils.internalError(
          "Failed to drop catalog " + catalogName + " under metalake " + metalakeName, e);
    }
  }
}
