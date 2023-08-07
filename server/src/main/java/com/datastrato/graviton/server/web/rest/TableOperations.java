/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server.web.rest;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.catalog.CatalogOperationDispatcher;
import com.datastrato.graviton.dto.requests.TableCreateRequest;
import com.datastrato.graviton.dto.requests.TableUpdateRequest;
import com.datastrato.graviton.dto.requests.TableUpdatesRequest;
import com.datastrato.graviton.dto.responses.DropResponse;
import com.datastrato.graviton.dto.responses.EntityListResponse;
import com.datastrato.graviton.dto.responses.TableResponse;
import com.datastrato.graviton.exceptions.NoSuchSchemaException;
import com.datastrato.graviton.exceptions.NoSuchTableException;
import com.datastrato.graviton.exceptions.TableAlreadyExistsException;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.TableChange;
import com.datastrato.graviton.server.web.Utils;
import com.google.common.base.Preconditions;
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
import org.eclipse.jetty.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables")
public class TableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(TableOperations.class);

  private final CatalogOperationDispatcher dispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public TableOperations(CatalogOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @GET
  @Produces("application/vnd.graviton.v1+json")
  public Response listTables(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema) {
    try {
      Namespace ns = tableNS(metalake, catalog, schema);
      NameIdentifier[] idents = dispatcher.listTables(ns);
      return Utils.ok(new EntityListResponse(idents));

    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate arguments", e);
      return Utils.illegalArguments("Failed to validate arguments", e);

    } catch (NoSuchSchemaException e) {
      LOG.error("Schema {} does not exist, fail to list tables", schema);
      return Utils.notFound("Schema " + schema + " does not exist, fail to list tables", e);

    } catch (Exception e) {
      LOG.error("Failed to list tables under schema {}", schema, e);
      return Utils.internalError("Failed to list tables under schema " + schema, e);
    }
  }

  @POST
  @Produces("application/vnd.graviton.v1+json")
  public Response createTable(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      TableCreateRequest request) {
    try {
      request.validate();
    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate TableCreateRequest arguments {}", request, e);
      return Utils.illegalArguments("Failed to validate TableCreateRequest arguments", e);
    }

    try {
      NameIdentifier ident = tableIdentifier(metalake, catalog, schema, request.getName());
      Table table =
          dispatcher.createTable(
              ident, request.getColumns(), request.getComment(), request.getProperties());
      return Utils.ok(new TableResponse(DTOConverters.toDTO(table)));

    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate arguments", e);
      return Utils.illegalArguments("Failed to validate arguments", e);

    } catch (NoSuchSchemaException e) {
      LOG.error("Schema {} does not exist, fail to create table {}", schema, request.getName());
      return Utils.notFound(
          "Schema " + schema + " does not exist, fail to create table " + request.getName(), e);

    } catch (TableAlreadyExistsException e) {
      LOG.error("Table {} already exists under schema {}", request.getName(), schema);
      return Utils.alreadyExists(
          "Table " + request.getName() + " already exists under schema " + schema, e);

    } catch (Exception e) {
      LOG.error("Failed to create table {} under schema {}", request.getName(), schema, e);
      return Utils.internalError(
          "Failed to create table " + request.getName() + " under schema " + schema, e);
    }
  }

  @GET
  @Path("{table}")
  @Produces("application/vnd.graviton.v1+json")
  public Response loadTable(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("table") String table) {
    try {
      NameIdentifier ident = tableIdentifier(metalake, catalog, schema, table);
      Table t = dispatcher.loadTable(ident);
      return Utils.ok(new TableResponse(DTOConverters.toDTO(t)));

    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate arguments", e);
      return Utils.illegalArguments("Failed to validate arguments", e);

    } catch (NoSuchTableException e) {
      LOG.error("Table {} does not exist under schema {}", table, schema);
      return Utils.notFound("Table " + table + " does not exist under schema " + schema, e);

    } catch (Exception e) {
      LOG.error("Failed to load table {} under schema {}", table, schema, e);
      return Utils.internalError("Failed to load table " + table + " under schema " + schema, e);
    }
  }

  @PUT
  @Path("{table}")
  @Produces("application/vnd.graviton.v1+json")
  public Response alterTable(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("table") String table,
      TableUpdatesRequest request) {
    try {
      request.validate();
    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate Request arguments {}", request, e);
      return Utils.illegalArguments("Failed to validate TableAlterRequest arguments", e);
    }

    try {
      NameIdentifier ident = tableIdentifier(metalake, catalog, schema, table);
      TableChange[] changes =
          request.getUpdates().stream()
              .map(TableUpdateRequest::tableChange)
              .toArray(TableChange[]::new);
      Table t = dispatcher.alterTable(ident, changes);
      return Utils.ok(new TableResponse(DTOConverters.toDTO(t)));

    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate arguments", e);
      return Utils.illegalArguments("Failed to validate arguments", e);

    } catch (NoSuchTableException e) {
      LOG.error("Table {} does not exist under schema {}", table, schema);
      return Utils.notFound("Table " + table + " does not exist under schema " + schema, e);

    } catch (Exception e) {
      LOG.error("Failed to alter table {} under schema {}", table, schema, e);
      return Utils.internalError("Failed to alter table " + table + " under schema " + schema, e);
    }
  }

  @DELETE
  @Path("{table}")
  @Produces("application/vnd.graviton.v1+json")
  public Response dropTable(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("table") String table) {
    try {
      NameIdentifier ident = tableIdentifier(metalake, catalog, schema, table);
      boolean dropped = dispatcher.dropTable(ident);
      if (!dropped) {
        LOG.warn("Failed to drop table {} under schema {}", table, schema);
      }

      return Utils.ok(new DropResponse(dropped));

    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate arguments", e);
      return Utils.illegalArguments("Failed to validate arguments", e);

    } catch (Exception e) {
      LOG.error("Failed to drop table {} under schema {}", table, schema, e);
      return Utils.internalError("Failed to drop table " + table + " under schema " + schema, e);
    }
  }

  private static Namespace tableNS(String metalake, String catalog, String schema) {
    Preconditions.checkArgument(
        StringUtil.isNotBlank(metalake), "metalake name: %s is illegal", metalake);
    Preconditions.checkArgument(
        StringUtil.isNotBlank(catalog), "catalog name: %s is illegal", catalog);
    Preconditions.checkArgument(
        StringUtil.isNotBlank(schema), "schema name: %s is illegal", schema);

    return Namespace.of(metalake, catalog, schema);
  }

  private static NameIdentifier tableIdentifier(
      String metalake, String catalog, String schema, String table) {
    Preconditions.checkArgument(
        StringUtil.isNotBlank(metalake), "metalake name: %s is illegal", metalake);
    Preconditions.checkArgument(
        StringUtil.isNotBlank(catalog), "catalog name: %s is illegal", catalog);
    Preconditions.checkArgument(
        StringUtil.isNotBlank(schema), "schema name: %s is illegal", schema);
    Preconditions.checkArgument(StringUtil.isNotBlank(table), "table name: %s is illegal", table);

    return NameIdentifier.of(metalake, catalog, schema, table);
  }
}
