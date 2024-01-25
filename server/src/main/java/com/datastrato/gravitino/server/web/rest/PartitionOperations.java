/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.catalog.CatalogOperationDispatcher;
import com.datastrato.gravitino.dto.responses.PartitionNameListResponse;
import com.datastrato.gravitino.dto.responses.PartitionResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.datastrato.gravitino.server.web.Utils;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

@Path("/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}/partitions")
public class PartitionOperations {

  private final CatalogOperationDispatcher dispatcher;
  @Context private HttpServletRequest httpRequest;

  @Inject
  public PartitionOperations(CatalogOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-partition-name." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-partition-name", absolute = true)
  public Response listPartitionNames(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("table") String table) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier tableIdent = NameIdentifier.of(metalake, catalog, schema, table);
            Table loadTable = dispatcher.loadTable(tableIdent);
            String[] partitionNames = loadTable.supportPartitions().listPartitionNames();
            return Utils.ok(new PartitionNameListResponse(partitionNames));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handlePartitionException(OperationType.LIST, "", table, e);
    }
  }

  @GET
  @Path("{partition}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-partition." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-partition", absolute = true)
  public Response getPartition(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("table") String table,
      @PathParam("partition") String partition) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier tableIdent = NameIdentifier.of(metalake, catalog, schema, table);
            Table loadTable = dispatcher.loadTable(tableIdent);
            Partition p = loadTable.supportPartitions().getPartition(partition);
            return Utils.ok(new PartitionResponse(DTOConverters.toDTO(p)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handlePartitionException(OperationType.LIST, "", table, e);
    }
  }
}
