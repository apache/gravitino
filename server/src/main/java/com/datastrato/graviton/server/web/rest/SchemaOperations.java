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
import com.datastrato.graviton.dto.responses.EntityListResponse;
import com.datastrato.graviton.dto.responses.SchemaResponse;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchSchemaException;
import com.datastrato.graviton.exceptions.NonEmptySchemaException;
import com.datastrato.graviton.exceptions.SchemaAlreadyExistsException;
import com.datastrato.graviton.rel.Schema;
import com.datastrato.graviton.rel.SchemaChange;
import com.datastrato.graviton.server.web.Utils;
import com.google.common.base.Preconditions;
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
import org.eclipse.jetty.util.StringUtil;
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
    try {
      Namespace ns = schemaNS(metalake, catalog);
      NameIdentifier[] idents = dispatcher.listSchemas(ns);
      return Utils.ok(new EntityListResponse(idents));

    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate arguments", e);
      return Utils.illegalArguments("Failed to validate arguments", e);

    } catch (NoSuchCatalogException e) {
      LOG.error("Catalog {} does not exist, fail to list schemas", catalog);
      return Utils.notFound("catalog " + catalog + " does not exist", e);

    } catch (Exception e) {
      LOG.error("Fail to list schemas under catalog {}", catalog, e);
      return Utils.internalError("Fail to list schemas under catalog " + catalog, e);
    }
  }

  @POST
  @Produces("application/vnd.graviton.v1+json")
  public Response createSchema(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      SchemaCreateRequest request) {
    try {
      request.validate();
    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate CreateSchemaRequest arguments {}", request, e);
      return Utils.illegalArguments(
          "Failed to validate CreateSchemaRequest arguments " + request, e);
    }

    try {
      NameIdentifier ident = schemaIdentifier(metalake, catalog, request.getName());
      Schema schema = dispatcher.createSchema(ident, request.getComment(), request.getProperties());
      return Utils.ok(new SchemaResponse(DTOConverters.toDTO(schema)));

    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate arguments", e);
      return Utils.illegalArguments("Failed to validate arguments", e);

    } catch (NoSuchCatalogException e) {
      LOG.error(
          "Catalog {} does not exist, fail to create schema {}", catalog, request.getName(), e);
      return Utils.notFound(
          "Catalog " + catalog + " does not exist, fail to create schema " + request.getName(), e);

    } catch (SchemaAlreadyExistsException e) {
      LOG.error("Schema {} already exists under {}", request.getComment(), catalog, e);
      return Utils.alreadyExists(
          "Schema " + request.getName() + " already exists under catalog " + catalog, e);

    } catch (Exception e) {
      LOG.error("Fail to create schema {} under catalog {}", request.getName(), catalog, e);
      return Utils.internalError(
          "Fail to create schema " + request.getName() + " under catalog " + catalog, e);
    }
  }

  @GET
  @Path("/{schema}")
  @Produces("application/vnd.graviton.v1+json")
  public Response loadSchema(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema) {
    try {
      NameIdentifier ident = schemaIdentifier(metalake, catalog, schema);
      Schema s = dispatcher.loadSchema(ident);
      return Utils.ok(new SchemaResponse(DTOConverters.toDTO(s)));

    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate arguments", e);
      return Utils.illegalArguments("Failed to validate arguments", e);

    } catch (NoSuchSchemaException e) {
      LOG.error("Schema {} does not exist under catalog {}", schema, catalog);
      return Utils.notFound("Schema " + schema + " does not exist under catalog " + catalog, e);

    } catch (Exception e) {
      LOG.error("Fail to load schema {} under catalog {}", schema, catalog, e);
      return Utils.internalError("Fail to load schema " + schema + " under catalog " + catalog, e);
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
    NameIdentifier ident;
    try {
      ident = schemaIdentifier(metalake, catalog, schema);
    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate arguments", e);
      return Utils.illegalArguments("Failed to validate arguments", e);
    }

    try {
      request.validate();
    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate SchemaUpdatesRequest arguments {}", request, e);
      return Utils.illegalArguments(
          "Failed to validate SchemaUpdatesRequest arguments " + request, e);
    }

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
    try {
      NameIdentifier ident = NameIdentifier.of(metalake, catalog, schema);
      boolean dropped = dispatcher.dropSchema(ident, cascade);
      if (!dropped) {
        LOG.warn("Fail to drop schema {} under namespace {}", schema, ident.namespace());
      }

      return Utils.ok(new DropResponse(dropped));

    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate arguments {} {} {}", metalake, catalog, schema, e);
      return Utils.illegalArguments(
          "Failed to validate arguments " + metalake + " " + catalog + " " + schema, e);

    } catch (NonEmptySchemaException e) {
      LOG.error(
          "Schema {} is not empty under catalog {}, but cascade is set to true", schema, catalog);
      return Utils.nonEmpty(
          "Schema "
              + schema
              + " is not empty under catalog "
              + catalog
              + " , but "
              + "cascade is set to true",
          e);

    } catch (Exception e) {
      LOG.error("Fail to drop schema {} under catalog {}", schema, catalog, e);
      return Utils.internalError("Fail to drop schema " + schema + " under catalog " + catalog, e);
    }
  }

  private static Namespace schemaNS(String metalake, String catalog) {
    Preconditions.checkArgument(
        StringUtil.isNotBlank(metalake), "metalake name: %s is illegal", metalake);
    Preconditions.checkArgument(
        StringUtil.isNotBlank(catalog), "catalog name: %s is illegal", catalog);

    return Namespace.of(metalake, catalog);
  }

  private static NameIdentifier schemaIdentifier(String metalake, String catalog, String schema) {
    Preconditions.checkArgument(
        StringUtil.isNotBlank(metalake), "metalake name: %s is illegal", metalake);
    Preconditions.checkArgument(
        StringUtil.isNotBlank(catalog), "catalog name: %s is illegal", catalog);
    Preconditions.checkArgument(
        StringUtil.isNotBlank(schema), "schema name: %s is illegal", schema);

    return NameIdentifier.of(metalake, catalog, schema);
  }
}
