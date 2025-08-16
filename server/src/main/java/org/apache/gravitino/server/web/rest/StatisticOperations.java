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
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.dto.requests.StatisticsDropRequest;
import org.apache.gravitino.dto.requests.StatisticsUpdateRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.StatisticsListResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.IllegalStatisticNameException;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.stats.Statistic;
import org.apache.gravitino.stats.StatisticManager;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}/statistics")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class StatisticOperations {

  private static final Logger LOG = LoggerFactory.getLogger(StatisticOperations.class);

  @Context private HttpServletRequest httpRequest;

  private final StatisticManager statisticManager;
  private final TableDispatcher tableDispatcher;

  @Inject
  public StatisticOperations(StatisticManager statisticManager, TableDispatcher tableDispatcher) {
    this.statisticManager = statisticManager;
    this.tableDispatcher = tableDispatcher;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-table-stats." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-table-stats", absolute = true)
  public Response listTableStatistics(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("table") String table) {
    LOG.info(
        "Received list statistics request for table: {}.{}.{}.{}",
        metalake,
        catalog,
        schema,
        table);
    try {
      MetadataObject metadataObject =
          MetadataObjects.of(Lists.newArrayList(catalog, schema, table), MetadataObject.Type.TABLE);
      return Utils.doAs(
          httpRequest,
          () -> {

            // Load the table to import the tables metadata if the table is not created by Gravitino
            tableDispatcher.loadTable(MetadataObjectUtil.toEntityIdent(metalake, metadataObject));

            List<Statistic> statistics = statisticManager.listStatistics(metalake, metadataObject);
            return Utils.ok(
                new StatisticsListResponse(
                    DTOConverters.toDTOs(statistics.toArray(new Statistic[0]))));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleStatisticException(OperationType.LIST, table, schema, e);
    }
  }

  @PUT
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "update-table-stats." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "update-table-stats", absolute = true)
  public Response updateTableStatistics(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("table") String table,
      StatisticsUpdateRequest request) {
    try {
      LOG.info(
          "Received update statistics request for table: {}.{}.{}.{}",
          metalake,
          catalog,
          schema,
          table);
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            MetadataObject metadataObject =
                MetadataObjects.of(
                    Lists.newArrayList(catalog, schema, table), MetadataObject.Type.TABLE);
            Map<String, StatisticValue<?>> statisticMaps = Maps.newHashMap();
            for (Map.Entry<String, StatisticValue<?>> entry : request.getUpdates().entrySet()) {
              // Current we only support custom statistics
              if (!entry.getKey().startsWith(Statistic.CUSTOM_PREFIX)) {
                throw new IllegalStatisticNameException(
                    "Statistic name must start with %s , but got: %s",
                    Statistic.CUSTOM_PREFIX, entry.getKey());
              }

              statisticMaps.put(entry.getKey(), entry.getValue());
            }

            // Load the table to import the tables metadata if the table is not created by Gravitino
            tableDispatcher.loadTable(MetadataObjectUtil.toEntityIdent(metalake, metadataObject));

            statisticManager.updateStatistics(metalake, metadataObject, statisticMaps);
            return Utils.ok(new BaseResponse(0));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleStatisticException(
          OperationType.UPDATE, StringUtils.join(request.getUpdates().keySet(), ","), table, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-table-stats." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-table-stats", absolute = true)
  public Response dropTableStatistics(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("table") String table,
      StatisticsDropRequest request) {
    try {
      LOG.info(
          "Received drop statistics request for table: {}.{}.{}.{}",
          metalake,
          catalog,
          schema,
          table);
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();

            MetadataObject metadataObject =
                MetadataObjects.of(
                    Lists.newArrayList(catalog, schema, table), MetadataObject.Type.TABLE);
            // Load the table to import the tables metadata if the table is not created by Gravitino
            tableDispatcher.loadTable(MetadataObjectUtil.toEntityIdent(metalake, metadataObject));

            statisticManager.dropStatistics(
                metalake, metadataObject, Lists.newArrayList(request.getNames()));
            return Utils.ok(new DropResponse(true));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleStatisticException(
          OperationType.DROP, StringUtils.join(request.getNames(), ","), table, e);
    }
  }
}
