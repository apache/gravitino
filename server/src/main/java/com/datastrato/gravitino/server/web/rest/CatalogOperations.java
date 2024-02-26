/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.CatalogChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.CatalogManager;
import com.datastrato.gravitino.dto.requests.CatalogCreateRequest;
import com.datastrato.gravitino.dto.requests.CatalogUpdateRequest;
import com.datastrato.gravitino.dto.requests.CatalogUpdatesRequest;
import com.datastrato.gravitino.dto.responses.CatalogResponse;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.EntityListResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.lock.LockType;
import com.datastrato.gravitino.lock.TreeLockUtils;
import com.datastrato.gravitino.server.web.Utils;
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
  @Produces("application/vnd.gravitino.v1+json")
  public Response listCatalogs(@PathParam("metalake") String metalake) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            Namespace catalogNS = Namespace.ofCatalog(metalake);
            // Lock the root and the metalake with WRITE lock to ensure the consistency of the list.
            NameIdentifier[] idents =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifier.of(metalake),
                    LockType.WRITE,
                    () -> manager.listCatalogs(catalogNS));
            return Utils.ok(new EntityListResponse(idents));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleCatalogException(OperationType.LIST, "", metalake, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  public Response createCatalog(
      @PathParam("metalake") String metalake, CatalogCreateRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident = NameIdentifier.ofCatalog(metalake, request.getName());
            Catalog catalog =
                TreeLockUtils.doWithTreeLock(
                    ident,
                    LockType.WRITE,
                    () ->
                        manager.createCatalog(
                            ident,
                            request.getType(),
                            request.getProvider(),
                            request.getComment(),
                            request.getProperties()));
            return Utils.ok(new CatalogResponse(DTOConverters.toDTO(catalog)));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleCatalogException(
          OperationType.CREATE, request.getName(), metalake, e);
    }
  }

  @GET
  @Path("{catalog}")
  @Produces("application/vnd.gravitino.v1+json")
  public Response loadCatalog(
      @PathParam("metalake") String metalakeName, @PathParam("catalog") String catalogName) {
    try {
      NameIdentifier ident = NameIdentifier.ofCatalog(metalakeName, catalogName);
      Catalog catalog =
          TreeLockUtils.doWithTreeLock(ident, LockType.READ, () -> manager.loadCatalog(ident));
      return Utils.ok(new CatalogResponse(DTOConverters.toDTO(catalog)));

    } catch (Exception e) {
      return ExceptionHandlers.handleCatalogException(
          OperationType.LOAD, catalogName, metalakeName, e);
    }
  }

  @PUT
  @Path("{catalog}")
  @Produces("application/vnd.gravitino.v1+json")
  public Response alterCatalog(
      @PathParam("metalake") String metalakeName,
      @PathParam("catalog") String catalogName,
      CatalogUpdatesRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident = NameIdentifier.ofCatalog(metalakeName, catalogName);
            CatalogChange[] changes =
                request.getUpdates().stream()
                    .map(CatalogUpdateRequest::catalogChange)
                    .toArray(CatalogChange[]::new);
            Catalog catalog =
                TreeLockUtils.doWithTreeLock(
                    ident, LockType.WRITE, () -> manager.alterCatalog(ident, changes));
            return Utils.ok(new CatalogResponse(DTOConverters.toDTO(catalog)));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleCatalogException(
          OperationType.ALTER, catalogName, metalakeName, e);
    }
  }

  @DELETE
  @Path("{catalog}")
  @Produces("application/vnd.gravitino.v1+json")
  public Response dropCatalog(
      @PathParam("metalake") String metalakeName, @PathParam("catalog") String catalogName) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifier.ofCatalog(metalakeName, catalogName);
            boolean dropped =
                TreeLockUtils.doWithTreeLock(
                    ident, LockType.WRITE, () -> manager.dropCatalog(ident));
            if (!dropped) {
              LOG.warn("Failed to drop catalog {} under metalake {}", catalogName, metalakeName);
            }

            return Utils.ok(new DropResponse(dropped));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleCatalogException(
          OperationType.DROP, catalogName, metalakeName, e);
    }
  }
}
