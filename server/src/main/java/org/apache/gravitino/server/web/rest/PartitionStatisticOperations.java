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

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.dto.requests.PartitionStatsDropRequest;
import org.apache.gravitino.dto.requests.PartitionStatsUpdateRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.PartitionStatsListResponse;
import org.apache.gravitino.dto.responses.PartitionStatsUpdateResponse;
import org.apache.gravitino.dto.stats.StatisticDTO;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.stats.PartitionStatisticManager;
import org.apache.gravitino.stats.Statistic;
import org.apache.gravitino.stats.StatisticValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path(
    "/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}/statistics/partitions")
public class PartitionStatisticOperations {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionStatisticOperations.class);

  @Context private HttpServletRequest httpRequest;

  private final PartitionStatisticManager partitionStatisticManager;

  @Inject
  public PartitionStatisticOperations(PartitionStatisticManager partitionStatisticManager) {
    this.partitionStatisticManager = partitionStatisticManager;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-partition-stats." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-partition-stats", absolute = true)
  public Response listPartitionStatistics(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("table") String table,
      @QueryParam("from") String fromPartitionName,
      @QueryParam("to") String toPartitionName) {
    LOG.info(
        "Listing partition statistics for table: {} {}.{}.{} from {} to {}",
        metalake,
        catalog,
        schema,
        table,
        fromPartitionName,
        toPartitionName);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            MetadataObject metadataObject =
                MetadataObjects.of(
                    Lists.newArrayList(catalog, schema, table), MetadataObject.Type.TABLE);
            Map<String, List<Statistic>> statistics =
                partitionStatisticManager.listPartitionStatistics(
                    metalake, metadataObject, fromPartitionName, toPartitionName);
            Map<String, List<StatisticDTO>> statisticsDTOs = Maps.newHashMap();
            statistics.forEach(
                (key, value) ->
                    statisticsDTOs.put(
                        key,
                        Lists.newArrayList(DTOConverters.toDTOs(value.toArray(new Statistic[0])))));
            return Utils.ok(new PartitionStatsListResponse(statisticsDTOs));
          });
    } catch (Exception e) {
      LOG.error(
          "Error listing partition statistics for table: {} {}.{}.{}",
          metalake,
          catalog,
          schema,
          table,
          e);
      return ExceptionHandlers.handlePartitionStatsException(
          OperationType.LIST, "[" + fromPartitionName + "," + toPartitionName + ")", table, e);
    }
  }

  @PUT
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "update-partitions-stats." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "update-partitions-stats", absolute = true)
  public Response updatePartitionStatistics(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("table") String table,
      PartitionStatsUpdateRequest request) {
    LOG.info(
        "Updating partition statistics for table: {} {}.{}.{}", metalake, catalog, schema, table);
    try {
      request.validate();

      return Utils.doAs(
          httpRequest,
          () -> {
            Map<String, Map<String, StatisticValue<?>>> updates = request.getUpdates();
            partitionStatisticManager.updatePartitionStatistics(
                metalake,
                MetadataObjects.of(
                    Lists.newArrayList(catalog, schema, table), MetadataObject.Type.TABLE),
                updates);
            Map<String, List<StatisticDTO>> statisticsDTOs = Maps.newHashMap();
            for (Map.Entry<String, Map<String, StatisticValue<?>>> e : updates.entrySet()) {
              String partition = e.getKey();
              Map<String, StatisticValue<?>> stats = e.getValue();
              statisticsDTOs.put(
                  partition,
                  stats.entrySet().stream()
                      .map(
                          entry ->
                              StatisticDTO.builder()
                                  .withName(entry.getKey())
                                  .withValue(Optional.of(entry.getValue()))
                                  .build())
                      .collect(Collectors.toList()));
            }
            return Utils.ok(new PartitionStatsUpdateResponse(statisticsDTOs));
          });
    } catch (Exception e) {
      LOG.error(
          "Error updating partition statistics for table: {} {}.{}.{}",
          metalake,
          catalog,
          schema,
          table,
          e);
      String partitions = StringUtils.joinWith(",", request.getUpdates().keySet());
      return ExceptionHandlers.handlePartitionStatsException(
          OperationType.UPDATE, partitions, table, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-partitions-stats." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-partitions-stats", absolute = true)
  public Response dropPartitionStatistics(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("table") String table,
      PartitionStatsDropRequest request) {
    LOG.info(
        "Dropping partition statistics for table: {} {}.{}.{}", metalake, catalog, schema, table);
    try {
      request.validate();

      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new DropResponse(
                      partitionStatisticManager.dropPartitionStatistics(
                          metalake,
                          MetadataObjects.of(
                              Lists.newArrayList(catalog, schema, table),
                              MetadataObject.Type.TABLE),
                          request.getPartitionStatistics()))));
    } catch (Exception e) {
      LOG.error(
          "Error dropping partition statistics for table: {} {}.{}.{}",
          metalake,
          catalog,
          schema,
          table,
          e);
      String partitions = StringUtils.joinWith(",", request.getPartitionStatistics().keySet());
      return ExceptionHandlers.handlePartitionStatsException(
          OperationType.DROP, partitions, table, e);
    }
  }
}
