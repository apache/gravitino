/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.server.web.rest;

import static org.apache.gravitino.dto.util.DTOConverters.fromDTO;
import static org.apache.gravitino.dto.util.DTOConverters.toDTOs;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.PartitionDispatcher;
import org.apache.gravitino.dto.rel.partitions.PartitionDTO;
import org.apache.gravitino.dto.requests.AddPartitionsRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.PartitionListResponse;
import org.apache.gravitino.dto.responses.PartitionNameListResponse;
import org.apache.gravitino.dto.responses.PartitionResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.server.web.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}/partitions")
public class PartitionOperations {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionOperations.class);

  private final PartitionDispatcher dispatcher;
  @Context private HttpServletRequest httpRequest;

  @Inject
  public PartitionOperations(PartitionDispatcher dispatcher) {
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
    LOG.info(
        "Received list partition {} request for table: {}.{}.{}.{}",
        verbose ? "infos" : "names",
        metalake,
        catalog,
        schema,
        table);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier tableIdent = NameIdentifier.of(metalake, catalog, schema, table);
            if (verbose) {
              Partition[] partitions = dispatcher.listPartitions(tableIdent);
              Response response = Utils.ok(new PartitionListResponse(toDTOs(partitions)));
              LOG.info(
                  "List {} partitions in table {}.{}.{}.{}",
                  partitions.length,
                  metalake,
                  catalog,
                  schema,
                  table);
              return response;
            } else {
              String[] partitionNames = dispatcher.listPartitionNames(tableIdent);
              Response response = Utils.ok(new PartitionNameListResponse((partitionNames)));
              LOG.info(
                  "List {} partition names in table {}.{}.{}.{}",
                  partitionNames.length,
                  metalake,
                  catalog,
                  schema,
                  table);
              return response;
            }
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
    LOG.info(
        "Received get partition request for partition[{}] of table[{}.{}.{}.{}]",
        partition,
        metalake,
        catalog,
        schema,
        table);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier tableIdent = NameIdentifier.of(metalake, catalog, schema, table);
            Partition p = dispatcher.getPartition(tableIdent, partition);
            Response response = Utils.ok(new PartitionResponse(DTOConverters.toDTO(p)));
            LOG.info(
                "Got partition[{}] in table[{}.{}.{}.{}]",
                partition,
                metalake,
                catalog,
                schema,
                table);
            return response;
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
    LOG.info(
        "Received add {} partition(s) request for table {}.{}.{}.{} ",
        request.getPartitions().length,
        metalake,
        catalog,
        schema,
        table);
    Preconditions.checkArgument(
        request.getPartitions().length == 1, "Only one partition is supported");

    request.validate();

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier tableIdent = NameIdentifier.of(metalake, catalog, schema, table);
            Partition p = dispatcher.addPartition(tableIdent, fromDTO(request.getPartitions()[0]));
            Response response =
                Utils.ok(new PartitionListResponse(new PartitionDTO[] {DTOConverters.toDTO(p)}));
            LOG.info(
                "Added {} partition(s) to table {}.{}.{}.{} ", 1, metalake, catalog, schema, table);
            return response;
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
      @QueryParam("purge") @DefaultValue("false") boolean purge) {
    LOG.info(
        "Received {} partition request for partition[{}] of table[{}.{}.{}.{}]",
        purge ? "purge" : "drop",
        partition,
        metalake,
        catalog,
        schema,
        table);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier tableIdent = NameIdentifier.of(metalake, catalog, schema, table);
            boolean dropped =
                purge
                    ? dispatcher.purgePartition(tableIdent, partition)
                    : dispatcher.dropPartition(tableIdent, partition);
            if (!dropped) {
              LOG.warn(
                  "Failed to drop partition {} under table {} under schema {}",
                  partition,
                  table,
                  schema);
            }
            Response response = Utils.ok(new DropResponse(dropped));
            LOG.info(
                "Partition {} {} in table {}.{}.{}.{}",
                partition,
                purge ? "purged" : "dropped",
                metalake,
                catalog,
                schema,
                table);
            return response;
          });
    } catch (Exception e) {
      return ExceptionHandlers.handlePartitionException(OperationType.DROP, "", table, e);
    }
  }
}
