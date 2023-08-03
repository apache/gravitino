/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server.web.rest;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.catalog.CatalogOperationDispatcher;
import com.datastrato.graviton.dto.requests.SchemaCreateRequest;
import com.datastrato.graviton.dto.requests.SchemaUpdateRequest;
import com.datastrato.graviton.dto.requests.SchemaUpdatesRequest;
import com.datastrato.graviton.dto.responses.DropResponse;
import com.datastrato.graviton.dto.responses.SchemaListResponse;
import com.datastrato.graviton.dto.responses.SchemaResponse;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchSchemaException;
import com.datastrato.graviton.exceptions.NonEmptySchemaException;
import com.datastrato.graviton.exceptions.SchemaAlreadyExistsException;
import com.datastrato.graviton.rel.Schema;
import com.datastrato.graviton.rel.SchemaChange;
import com.datastrato.graviton.server.web.Utils;
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

  private final CatalogOperationDispatcher dispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public SchemaOperations(CatalogOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @GET
  @Produces("application/vnd.graviton.v1+json")
  public Response listSchemas(
      @PathParam("metalake") String metalake, @PathParam("catalog") String catalog) {
    if (metalake == null || metalake.isEmpty()) {
      LOG.error("Metalake name is null or empty");
      return Utils.illegalArguments("Metalake name is illegal");
    }

    if (catalog == null || catalog.isEmpty()) {
      LOG.error("Catalog name is null or empty");
      return Utils.illegalArguments("Catalog name is illegal");
    }

    Namespace ns = Namespace.of(metalake, catalog);
    try {
      NameIdentifier[] idents = dispatcher.listSchemas(ns);
      return Utils.ok(new SchemaListResponse(idents));

    } catch (NoSuchCatalogException e) {
      LOG.error("Catalog {} does not exist, fail to list schemas", ns);
      return Utils.notFound("catalog " + ns + " does not exist", e);

    } catch (Exception e) {
      LOG.error("Fail to list schemas under namespace {}", ns, e);
      return Utils.internalError("Fail to list schemas under namespace " + ns, e);
    }
  }

  @POST
  @Produces("application/vnd.graviton.v1+json")
  public Response createSchema(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      SchemaCreateRequest request) {
    if (metalake == null || metalake.isEmpty()) {
      LOG.error("Metalake name is null or empty");
      return Utils.illegalArguments("Metalake name is illegal");
    }

    if (catalog == null || catalog.isEmpty()) {
      LOG.error("Catalog name is null or empty");
      return Utils.illegalArguments("Catalog name is illegal");
    }

    try {
      request.validate();
    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate CreateSchemaRequest arguments {}", request, e);
      return Utils.illegalArguments(
          "Failed to validate CreateSchemaRequest arguments " + request, e);
    }

    NameIdentifier ident = NameIdentifier.of(metalake, catalog, request.getName());
    try {
      Schema schema = dispatcher.createSchema(ident, request.getComment(), request.getProperties());
      return Utils.ok(new SchemaResponse(DTOConverters.toDTO(schema)));

    } catch (NoSuchCatalogException e) {
      LOG.error(
          "Catalog {} does not exist, fail to create schema {}",
          ident.namespace(),
          ident.name(),
          e);
      return Utils.notFound(
          "Catalog " + ident.namespace() + " does not exist, fail to create schema " + ident.name(),
          e);

    } catch (SchemaAlreadyExistsException e) {
      LOG.error("Schema {} already exists under {}", ident.name(), ident.namespace(), e);
      return Utils.alreadyExists(
          "Schema " + ident.name() + " already exists under " + ident.namespace(), e);

    } catch (Exception e) {
      LOG.error("Fail to create schema {} under namespace {}", ident, request, e);
      return Utils.internalError("Fail to create schema " + ident + " under namespace " + ident, e);
    }
  }

  @GET
  @Path("/{schema}")
  @Produces("application/vnd.graviton.v1+json")
  public Response loadSchema(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema) {
    if (metalake == null || metalake.isEmpty()) {
      LOG.error("Metalake name is null or empty");
      return Utils.illegalArguments("Metalake name is illegal");
    }

    if (catalog == null || catalog.isEmpty()) {
      LOG.error("Catalog name is null or empty");
      return Utils.illegalArguments("Catalog name is illegal");
    }

    if (schema == null || schema.isEmpty()) {
      LOG.error("Schema name is null or empty");
      return Utils.illegalArguments("Schema name is illegal");
    }

    NameIdentifier ident = NameIdentifier.of(metalake, catalog, schema);
    try {
      Schema s = dispatcher.loadSchema(ident);
      return Utils.ok(new SchemaResponse(DTOConverters.toDTO(s)));

    } catch (NoSuchSchemaException e) {
      LOG.error("Schema {} does not exist under namespace {}", schema, ident.namespace());
      return Utils.notFound(
          "Schema " + schema + " does not exist under namespace " + ident.namespace(), e);

    } catch (Exception e) {
      LOG.error("Fail to load schema {} under namespace {}", schema, ident.namespace(), e);
      return Utils.internalError(
          "Fail to load schema " + schema + " under namespace " + ident.namespace(), e);
    }
  }

  @PUT
  @Path("/{schema}")
  @Produces("application/vnd.graviton.v1+json")
  public Response alterSchema(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      SchemaUpdatesRequest request) {
    if (metalake == null || metalake.isEmpty()) {
      LOG.error("Metalake name is null or empty");
      return Utils.illegalArguments("Metalake name is illegal");
    }

    if (catalog == null || catalog.isEmpty()) {
      LOG.error("Catalog name is null or empty");
      return Utils.illegalArguments("Catalog name is illegal");
    }

    if (schema == null || schema.isEmpty()) {
      LOG.error("Schema name is null or empty");
      return Utils.illegalArguments("Schema name is illegal");
    }

    try {
      request.validate();
    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate SchemaUpdatesRequest arguments {}", request, e);
      return Utils.illegalArguments(
          "Failed to validate SchemaUpdatesRequest arguments " + request, e);
    }

    NameIdentifier ident = NameIdentifier.of(metalake, catalog, schema);
    SchemaChange[] changes =
        request.getUpdates().stream()
            .map(SchemaUpdateRequest::schemaChange)
            .toArray(SchemaChange[]::new);
    try {
      Schema s = dispatcher.alterSchema(ident, changes);
      return Utils.ok(new SchemaResponse(DTOConverters.toDTO(s)));

    } catch (NoSuchSchemaException e) {
      LOG.error("Schema {} does not exist under namespace {}", schema, ident.namespace());
      return Utils.notFound(
          "Schema " + schema + " does not exist under namespace " + ident.namespace(), e);

    } catch (Exception e) {
      LOG.error("Fail to alter schema {} under namespace {}", schema, ident.namespace(), e);
      return Utils.internalError(
          "Fail to alter schema " + schema + " under namespace " + ident.namespace(), e);
    }
  }

  @DELETE
  @Path("/{schema}")
  @Produces("application/vnd.graviton.v1+json")
  public Response dropSchema(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @DefaultValue("false") @QueryParam("cascade") boolean cascade) {
    if (metalake == null || metalake.isEmpty()) {
      LOG.error("Metalake name is null or empty");
      return Utils.illegalArguments("Metalake name is illegal");
    }

    if (catalog == null || catalog.isEmpty()) {
      LOG.error("Catalog name is null or empty");
      return Utils.illegalArguments("Catalog name is illegal");
    }

    if (schema == null || schema.isEmpty()) {
      LOG.error("Schema name is null or empty");
      return Utils.illegalArguments("Schema name is illegal");
    }

    NameIdentifier ident = NameIdentifier.of(metalake, catalog, schema);
    try {
      boolean dropped = dispatcher.dropSchema(ident, cascade);
      if (!dropped) {
        LOG.warn("Fail to drop schema {} under namespace {}", schema, ident.namespace());
      }

      return Utils.ok(new DropResponse(dropped));
    } catch (NonEmptySchemaException e) {
      LOG.error(
          "Schema {} is not empty under namespace {}, but cascade is set to true",
          schema,
          ident.namespace());
      return Utils.nonEmpty(
          "Schema "
              + schema
              + " is not empty under namespace "
              + ident.namespace()
              + " , but "
              + "cascade is set to true",
          e);

    } catch (Exception e) {
      LOG.error("Fail to drop schema {} under namespace {}", schema, ident.namespace(), e);
      return Utils.internalError(
          "Fail to drop schema " + schema + " under namespace " + ident.namespace(), e);
    }
  }
}
