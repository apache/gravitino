/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.auth.Authenticator;
import com.datastrato.gravitino.catalog.CatalogOperationDispatcher;
import com.datastrato.gravitino.dto.requests.SchemaCreateRequest;
import com.datastrato.gravitino.dto.requests.SchemaUpdateRequest;
import com.datastrato.gravitino.dto.requests.SchemaUpdatesRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.EntityListResponse;
import com.datastrato.gravitino.dto.responses.SchemaResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.server.web.Utils;
import com.datastrato.gravitino.utils.Constants;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
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

  private final CatalogOperationDispatcher dispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public SchemaOperations(CatalogOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  public Response listSchemas(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog) {
    try {
      Namespace schemaNS = Namespace.ofSchema(metalake, catalog);
      NameIdentifier[] idents = dispatcher.listSchemas(schemaNS);
      return Utils.ok(new EntityListResponse(idents));

    } catch (Exception e) {
      return ExceptionHandlers.handleSchemaException(OperationType.LIST, "", catalog, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  public Response createSchema(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      SchemaCreateRequest request) {
    try {
      request.validate();
      NameIdentifier ident = NameIdentifier.ofSchema(metalake, catalog, request.getName());
      Schema schema = dispatcher.createSchema(ident, request.getComment(), request.getProperties());
      return Utils.ok(new SchemaResponse(DTOConverters.toDTO(schema)));

    } catch (Exception e) {
      return ExceptionHandlers.handleSchemaException(
          OperationType.CREATE, request.getName(), catalog, e);
    }
  }

  @GET
  @Path("/{schema}")
  @Produces("application/vnd.gravitino.v1+json")
  public Response loadSchema(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema) {
    try {
      NameIdentifier ident = NameIdentifier.ofSchema(metalake, catalog, schema);
      Schema s = dispatcher.loadSchema(ident);
      return Utils.ok(new SchemaResponse(DTOConverters.toDTO(s)));

    } catch (Exception e) {
      return ExceptionHandlers.handleSchemaException(OperationType.LOAD, schema, catalog, e);
    }
  }

  @PUT
  @Path("/{schema}")
  @Produces("application/vnd.gravitino.v1+json")
  public Response alterSchema(
      @HeaderParam(Constants.HTTP_HEADER_NAME) String authData,
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      SchemaUpdatesRequest request) {
    try {
      request.validate();
      NameIdentifier ident = NameIdentifier.ofSchema(metalake, catalog, schema);
      SchemaChange[] changes =
          request.getUpdates().stream()
              .map(SchemaUpdateRequest::schemaChange)
              .toArray(SchemaChange[]::new);
      Schema s = dispatcher.alterSchema(ident, changes);
      return Utils.ok(new SchemaResponse(DTOConverters.toDTO(s)));

    } catch (Exception e) {
      return ExceptionHandlers.handleSchemaException(OperationType.ALTER, schema, catalog, e);
    }
  }

  @DELETE
  @Path("/{schema}")
  @Produces("application/vnd.gravitino.v1+json")
  public Response dropSchema(
      @HeaderParam(Constants.HTTP_HEADER_NAME) String authData,
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @DefaultValue("false") @QueryParam("cascade") boolean cascade) {
    try {
      NameIdentifier ident = NameIdentifier.ofSchema(metalake, catalog, schema);
      boolean dropped = dispatcher.dropSchema(ident, cascade);
      if (!dropped) {
        LOG.warn("Fail to drop schema {} under namespace {}", schema, ident.namespace());
      }

      return Utils.ok(new DropResponse(dropped));

    } catch (Exception e) {
      return ExceptionHandlers.handleSchemaException(OperationType.DROP, schema, catalog, e);
    }
  }
}
