/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.CatalogOperationDispatcher;
import com.datastrato.gravitino.dto.requests.FilesetCreateRequest;
import com.datastrato.gravitino.dto.requests.FilesetUpdateRequest;
import com.datastrato.gravitino.dto.requests.FilesetUpdatesRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.EntityListResponse;
import com.datastrato.gravitino.dto.responses.FilesetResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.lock.LockType;
import com.datastrato.gravitino.lock.TreeLockUtils;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.server.web.Utils;
import java.util.Optional;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/filesets")
public class FilesetOperations {

  private static final Logger LOG = LoggerFactory.getLogger(FilesetOperations.class);

  private final CatalogOperationDispatcher dispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public FilesetOperations(CatalogOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-fileset." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-fileset", absolute = true)
  public Response listFilesets(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            Namespace filesetNS = Namespace.ofFileset(metalake, catalog, schema);
            NameIdentifier[] idents =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifier.of(metalake, catalog, schema),
                    LockType.WRITE,
                    () -> dispatcher.listFilesets(filesetNS));
            return Utils.ok(new EntityListResponse(idents));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleFilesetException(OperationType.LIST, "", schema, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "create-fileset." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-fileset", absolute = true)
  public Response createFileset(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      FilesetCreateRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident =
                NameIdentifier.ofFileset(metalake, catalog, schema, request.getName());

            Fileset fileset =
                TreeLockUtils.doWithTreeLock(
                    ident,
                    LockType.WRITE,
                    () ->
                        dispatcher.createFileset(
                            ident,
                            request.getComment(),
                            Optional.ofNullable(request.getType()).orElse(Fileset.Type.MANAGED),
                            request.getStorageLocation(),
                            request.getProperties()));
            return Utils.ok(new FilesetResponse(DTOConverters.toDTO(fileset)));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleFilesetException(
          OperationType.CREATE, request.getName(), schema, e);
    }
  }

  @GET
  @Path("{fileset}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "load-fileset." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-fileset", absolute = true)
  public Response loadFileset(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("fileset") String fileset) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifier.ofFileset(metalake, catalog, schema, fileset);
            Fileset t =
                TreeLockUtils.doWithTreeLock(
                    ident, LockType.READ, () -> dispatcher.loadFileset(ident));
            return Utils.ok(new FilesetResponse(DTOConverters.toDTO(t)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleFilesetException(OperationType.LOAD, fileset, schema, e);
    }
  }

  @PUT
  @Path("{fileset}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "alter-fileset." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-fileset", absolute = true)
  public Response alterFileset(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("fileset") String fileset,
      FilesetUpdatesRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident = NameIdentifier.ofFileset(metalake, catalog, schema, fileset);
            FilesetChange[] changes =
                request.getUpdates().stream()
                    .map(FilesetUpdateRequest::filesetChange)
                    .toArray(FilesetChange[]::new);
            Fileset t =
                TreeLockUtils.doWithTreeLock(
                    ident, LockType.WRITE, () -> dispatcher.alterFileset(ident, changes));
            return Utils.ok(new FilesetResponse(DTOConverters.toDTO(t)));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleFilesetException(OperationType.ALTER, fileset, schema, e);
    }
  }

  @DELETE
  @Path("{fileset}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-fileset." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-fileset", absolute = true)
  public Response dropFileset(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("fileset") String fileset) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifier.ofFileset(metalake, catalog, schema, fileset);
            boolean dropped =
                TreeLockUtils.doWithTreeLock(
                    ident, LockType.WRITE, () -> dispatcher.dropFileset(ident));
            if (!dropped) {
              LOG.warn("Failed to drop fileset {} under schema {}", fileset, schema);
            }

            return Utils.ok(new DropResponse(dropped));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleFilesetException(OperationType.DROP, fileset, schema, e);
    }
  }
}
