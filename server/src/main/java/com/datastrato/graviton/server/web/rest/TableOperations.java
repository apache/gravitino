/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server.web.rest;

import static com.datastrato.graviton.dto.rel.PartitionUtils.toTransforms;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.catalog.CatalogOperationDispatcher;
import com.datastrato.graviton.dto.requests.TableCreateRequest;
import com.datastrato.graviton.dto.requests.TableUpdateRequest;
import com.datastrato.graviton.dto.requests.TableUpdatesRequest;
import com.datastrato.graviton.dto.responses.DropResponse;
import com.datastrato.graviton.dto.responses.EntityListResponse;
import com.datastrato.graviton.dto.responses.TableResponse;
import com.datastrato.graviton.dto.util.DTOConverters;
import com.datastrato.graviton.rel.Distribution;
import com.datastrato.graviton.rel.SortOrder;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.TableChange;
import com.datastrato.graviton.server.web.Utils;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
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
import javax.ws.rs.core.Response;
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
      Namespace tableNS = Namespace.ofTable(metalake, catalog, schema);
      NameIdentifier[] idents = dispatcher.listTables(tableNS);
      return Utils.ok(new EntityListResponse(idents));

    } catch (Exception e) {
      return ExceptionHandlers.handleTableException(OperationType.LIST, "", schema, e);
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
      NameIdentifier ident = NameIdentifier.ofTable(metalake, catalog, schema, request.getName());
      SortOrder[] sortOrders =
          com.datastrato.graviton.dto.util.DTOConverters.fromDTOs(request.getSortOrders());
      Distribution distribution =
          com.datastrato.graviton.dto.util.DTOConverters.fromDTO(request.getDistribution());

      Table table =
          dispatcher.createTable(
              ident,
              request.getColumns(),
              request.getComment(),
              request.getProperties(),
              toTransforms(request.getPartitions()),
              distribution,
              sortOrders);
      return Utils.ok(new TableResponse(DTOConverters.toDTO(table)));

    } catch (Exception e) {
      return ExceptionHandlers.handleTableException(
          OperationType.CREATE, request.getName(), schema, e);
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
      NameIdentifier ident = NameIdentifier.ofTable(metalake, catalog, schema, table);
      Table t = dispatcher.loadTable(ident);
      return Utils.ok(new TableResponse(DTOConverters.toDTO(t)));

    } catch (Exception e) {
      return ExceptionHandlers.handleTableException(OperationType.LOAD, table, schema, e);
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
      NameIdentifier ident = NameIdentifier.ofTable(metalake, catalog, schema, table);
      TableChange[] changes =
          request.getUpdates().stream()
              .map(TableUpdateRequest::tableChange)
              .toArray(TableChange[]::new);
      Table t = dispatcher.alterTable(ident, changes);
      return Utils.ok(new TableResponse(DTOConverters.toDTO(t)));

    } catch (Exception e) {
      return ExceptionHandlers.handleTableException(OperationType.ALTER, table, schema, e);
    }
  }

  @DELETE
  @Path("{table}")
  @Produces("application/vnd.graviton.v1+json")
  public Response dropTable(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("table") String table,
      @QueryParam("purge") @DefaultValue("false") boolean purge) {
    try {
      NameIdentifier ident = NameIdentifier.ofTable(metalake, catalog, schema, table);
      boolean dropped = purge ? dispatcher.purgeTable(ident) : dispatcher.dropTable(ident);
      if (!dropped) {
        LOG.warn("Failed to drop table {} under schema {}", table, schema);
      }

      return Utils.ok(new DropResponse(dropped));

    } catch (Exception e) {
      return ExceptionHandlers.handleTableException(OperationType.DROP, table, schema, e);
    }
  }
}
