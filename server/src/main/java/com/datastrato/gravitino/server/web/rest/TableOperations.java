/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import static com.datastrato.gravitino.dto.util.DTOConverters.fromDTO;
import static com.datastrato.gravitino.dto.util.DTOConverters.fromDTOs;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.CatalogOperationDispatcher;
import com.datastrato.gravitino.dto.requests.TableCreateRequest;
import com.datastrato.gravitino.dto.requests.TableUpdateRequest;
import com.datastrato.gravitino.dto.requests.TableUpdatesRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.EntityListResponse;
import com.datastrato.gravitino.dto.responses.TableResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.lock.LockType;
import com.datastrato.gravitino.lock.TreeLockUtils;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.server.web.Utils;
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
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-table", absolute = true)
  public Response listTables(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            Namespace tableNS = Namespace.ofTable(metalake, catalog, schema);
            NameIdentifier[] idents =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifier.of(metalake, catalog, schema),
                    LockType.WRITE,
                    () -> dispatcher.listTables(tableNS));
            return Utils.ok(new EntityListResponse(idents));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleTableException(OperationType.LIST, "", schema, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "create-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-table", absolute = true)
  public Response createTable(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      TableCreateRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident =
                NameIdentifier.ofTable(metalake, catalog, schema, request.getName());

            Table table =
                TreeLockUtils.doWithTreeLock(
                    ident,
                    LockType.WRITE,
                    () ->
                        dispatcher.createTable(
                            ident,
                            fromDTOs(request.getColumns()),
                            request.getComment(),
                            request.getProperties(),
                            fromDTOs(request.getPartitioning()),
                            fromDTO(request.getDistribution()),
                            fromDTOs(request.getSortOrders()),
                            fromDTOs(request.getIndexes())));
            return Utils.ok(new TableResponse(DTOConverters.toDTO(table)));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleTableException(
          OperationType.CREATE, request.getName(), schema, e);
    }
  }

  @GET
  @Path("{table}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "load-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-table", absolute = true)
  public Response loadTable(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("table") String table) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifier.ofTable(metalake, catalog, schema, table);
            Table t =
                TreeLockUtils.doWithTreeLock(
                    ident, LockType.READ, () -> dispatcher.loadTable(ident));
            return Utils.ok(new TableResponse(DTOConverters.toDTO(t)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleTableException(OperationType.LOAD, table, schema, e);
    }
  }

  @PUT
  @Path("{table}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "alter-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-table", absolute = true)
  public Response alterTable(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("table") String table,
      TableUpdatesRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident = NameIdentifier.ofTable(metalake, catalog, schema, table);
            TableChange[] changes =
                request.getUpdates().stream()
                    .map(TableUpdateRequest::tableChange)
                    .toArray(TableChange[]::new);
            Table t =
                TreeLockUtils.doWithTreeLock(
                    ident, LockType.WRITE, () -> dispatcher.alterTable(ident, changes));
            return Utils.ok(new TableResponse(DTOConverters.toDTO(t)));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleTableException(OperationType.ALTER, table, schema, e);
    }
  }

  @DELETE
  @Path("{table}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-table", absolute = true)
  public Response dropTable(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("table") String table,
      @QueryParam("purge") @DefaultValue("false") boolean purge) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifier.ofTable(metalake, catalog, schema, table);
            boolean dropped =
                TreeLockUtils.doWithTreeLock(
                    ident,
                    LockType.WRITE,
                    () -> purge ? dispatcher.purgeTable(ident) : dispatcher.dropTable(ident));
            if (!dropped) {
              LOG.warn("Failed to drop table {} under schema {}", table, schema);
            }

            return Utils.ok(new DropResponse(dropped));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleTableException(OperationType.DROP, table, schema, e);
    }
  }
}
