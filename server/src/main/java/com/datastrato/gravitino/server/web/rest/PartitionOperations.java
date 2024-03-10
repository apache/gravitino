/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import static com.datastrato.gravitino.dto.util.DTOConverters.fromDTO;
import static com.datastrato.gravitino.dto.util.DTOConverters.toDTOs;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.catalog.CatalogOperationDispatcher;
import com.datastrato.gravitino.dto.rel.partitions.PartitionDTO;
import com.datastrato.gravitino.dto.requests.AddPartitionsRequest;
import com.datastrato.gravitino.dto.requests.DropPartitionsRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.PartitionListResponse;
import com.datastrato.gravitino.dto.responses.PartitionNameListResponse;
import com.datastrato.gravitino.dto.responses.PartitionResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.lock.LockType;
import com.datastrato.gravitino.lock.TreeLockUtils;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.datastrato.gravitino.server.web.Utils;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}/partitions")
public class PartitionOperations {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionOperations.class);

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
      @PathParam("table") String table,
      @QueryParam("details") @DefaultValue("false") boolean verbose) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier tableIdent = NameIdentifier.of(metalake, catalog, schema, table);
            return TreeLockUtils.doWithTreeLock(
                tableIdent,
                LockType.READ,
                () -> {
                  Table loadTable = dispatcher.loadTable(tableIdent);
                  if (verbose) {
                    Partition[] partitions = loadTable.supportPartitions().listPartitions();
                    return Utils.ok(new PartitionListResponse(toDTOs(partitions)));
                  } else {
                    String[] partitionNames = loadTable.supportPartitions().listPartitionNames();
                    return Utils.ok(new PartitionNameListResponse((partitionNames)));
                  }
                });
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
            return TreeLockUtils.doWithTreeLock(
                tableIdent,
                LockType.READ,
                () -> {
                  Table loadTable = dispatcher.loadTable(tableIdent);
                  Partition p = loadTable.supportPartitions().getPartition(partition);
                  return Utils.ok(new PartitionResponse(DTOConverters.toDTO(p)));
                });
          });
    } catch (Exception e) {
      return ExceptionHandlers.handlePartitionException(OperationType.GET, "", table, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "add-partitions." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "add-partitions", absolute = true)
  public Response addPartitions(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("table") String table,
      AddPartitionsRequest request) {
    Preconditions.checkArgument(
        request.getPartitions().length == 1, "Only one partition is supported");

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier tableIdent = NameIdentifier.of(metalake, catalog, schema, table);
            return TreeLockUtils.doWithTreeLock(
                tableIdent,
                LockType.WRITE,
                () -> {
                  Table loadTable = dispatcher.loadTable(tableIdent);
                  Partition p =
                      loadTable
                          .supportPartitions()
                          .addPartition(fromDTO(request.getPartitions()[0]));
                  return Utils.ok(
                      new PartitionListResponse(new PartitionDTO[] {DTOConverters.toDTO(p)}));
                });
          });
    } catch (Exception e) {
      return ExceptionHandlers.handlePartitionException(OperationType.CREATE, "", table, e);
    }
  }

  @DELETE
  @Path("{partition}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-partition." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-partition", absolute = true)
  public Response dropPartition(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("table") String table,
      @PathParam("partition") String partition,
      @QueryParam("purge") @DefaultValue("false") boolean purge,
      @QueryParam("ifExists") @DefaultValue("false") boolean ifExists) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier tableIdent = NameIdentifier.of(metalake, catalog, schema, table);
            Table loadTable = dispatcher.loadTable(tableIdent);
            boolean dropped =
                purge
                    ? loadTable.supportPartitions().purgePartition(partition, ifExists)
                    : loadTable.supportPartitions().dropPartition(partition, ifExists);
            if (!dropped) {
              LOG.warn(
                  "Failed to drop partition {} under table {} under schema {}",
                  partition,
                  table,
                  schema);
            }
            return Utils.ok(new DropResponse(dropped));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handlePartitionException(OperationType.DROP, "", table, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-partitions." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @Path("/delete")
  @ResponseMetered(name = "drop-partitions", absolute = true)
  public Response dropPartitions(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("table") String table,
      @QueryParam("purge") @DefaultValue("false") boolean purge,
      @QueryParam("ifExists") @DefaultValue("false") boolean ifExists,
      DropPartitionsRequest request) {
    Preconditions.checkArgument(
        request.getPartitionNames().length == 1, "Only one partition is supported");
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier tableIdent = NameIdentifier.of(metalake, catalog, schema, table);
            Table loadTable = dispatcher.loadTable(tableIdent);
            boolean dropped =
                purge
                    ? loadTable
                        .supportPartitions()
                        .purgePartitions(Arrays.asList(request.getPartitionNames()), ifExists)
                    : loadTable
                        .supportPartitions()
                        .dropPartitions(Arrays.asList(request.getPartitionNames()), ifExists);
            return Utils.ok(new DropResponse(dropped));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handlePartitionException(OperationType.DROP, "", table, e);
    }
  }
}
