/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.SchemaDispatcher;
import com.datastrato.gravitino.dto.requests.SchemaCreateRequest;
import com.datastrato.gravitino.dto.requests.SchemaUpdateRequest;
import com.datastrato.gravitino.dto.requests.SchemaUpdatesRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.EntityListResponse;
import com.datastrato.gravitino.dto.responses.SchemaResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.lock.LockType;
import com.datastrato.gravitino.lock.TreeLockUtils;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.server.web.Utils;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/metalakes/{metalake}/catalogs/{catalog}/schemas")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class SchemaOperations {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaOperations.class);

  private final SchemaDispatcher dispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public SchemaOperations(SchemaDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-schema." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-schema", absolute = true)
  public Response listSchemas(
      @PathParam("metalake") String metalake, @PathParam("catalog") String catalog) {
    LOG.info("Received list schema request for catalog: {}.{}", metalake, catalog);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            Namespace schemaNS = Namespace.ofSchema(metalake, catalog);
            NameIdentifier[] idents =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifier.of(metalake, catalog),
                    LockType.READ,
                    () -> dispatcher.listSchemas(schemaNS));
            Response response = Utils.ok(new EntityListResponse(idents));
            LOG.info("List {} schemas in catalog {}.{}", idents.length, metalake, catalog);
            return response;
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleSchemaException(OperationType.LIST, "", catalog, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "create-schema." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-schema", absolute = true)
  public Response createSchema(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      SchemaCreateRequest request) {
    LOG.info("Received create schema request: {}.{}.{}", metalake, catalog, request.getName());
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident = NameIdentifier.ofSchema(metalake, catalog, request.getName());
            Schema schema =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifier.ofCatalog(metalake, catalog),
                    LockType.WRITE,
                    () ->
                        dispatcher.createSchema(
                            ident, request.getComment(), request.getProperties()));
            Response response = Utils.ok(new SchemaResponse(DTOConverters.toDTO(schema)));
            LOG.info("Schema created: {}.{}.{}", metalake, catalog, schema.name());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleSchemaException(
          OperationType.CREATE, request.getName(), catalog, e);
    }
  }

  @GET
  @Path("/{schema}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "load-schema." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-schema", absolute = true)
  public Response loadSchema(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema) {
    LOG.info("Received load schema request for schema: {}.{}.{}", metalake, catalog, schema);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifier.ofSchema(metalake, catalog, schema);
            Schema s =
                TreeLockUtils.doWithTreeLock(
                    ident, LockType.READ, () -> dispatcher.loadSchema(ident));
            Response response = Utils.ok(new SchemaResponse(DTOConverters.toDTO(s)));
            LOG.info("Schema loaded: {}.{}.{}", metalake, catalog, s.name());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleSchemaException(OperationType.LOAD, schema, catalog, e);
    }
  }

  @PUT
  @Path("/{schema}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "alter-schema." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-schema", absolute = true)
  public Response alterSchema(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      SchemaUpdatesRequest request) {
    LOG.info("Received alter schema request: {}.{}.{}", metalake, catalog, schema);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident = NameIdentifier.ofSchema(metalake, catalog, schema);
            SchemaChange[] changes =
                request.getUpdates().stream()
                    .map(SchemaUpdateRequest::schemaChange)
                    .toArray(SchemaChange[]::new);
            Schema s =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifier.ofCatalog(metalake, catalog),
                    LockType.WRITE,
                    () -> dispatcher.alterSchema(ident, changes));
            Response response = Utils.ok(new SchemaResponse(DTOConverters.toDTO(s)));
            LOG.info("Schema altered: {}.{}.{}", metalake, catalog, s.name());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleSchemaException(OperationType.ALTER, schema, catalog, e);
    }
  }

  @DELETE
  @Path("/{schema}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-schema." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-schema", absolute = true)
  public Response dropSchema(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @DefaultValue("false") @QueryParam("cascade") boolean cascade) {
    LOG.info("Received drop schema request: {}.{}.{}", metalake, catalog, schema);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifier.ofSchema(metalake, catalog, schema);
            boolean dropped =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifier.ofCatalog(metalake, catalog),
                    LockType.WRITE,
                    () -> dispatcher.dropSchema(ident, cascade));
            if (!dropped) {
              LOG.warn("Fail to drop schema {} under namespace {}", schema, ident.namespace());
            }

            Response response = Utils.ok(new DropResponse(dropped));
            LOG.info("Schema dropped: {}.{}.{}", metalake, catalog, schema);
            return response;
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleSchemaException(OperationType.DROP, schema, catalog, e);
    }
  }
}
