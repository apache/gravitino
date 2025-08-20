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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.dto.requests.PartitionStatisticsDropRequest;
import org.apache.gravitino.dto.requests.PartitionStatisticsUpdateRequest;
import org.apache.gravitino.dto.requests.StatisticsDropRequest;
import org.apache.gravitino.dto.requests.StatisticsUpdateRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.PartitionStatisticsListResponse;
import org.apache.gravitino.dto.responses.StatisticListResponse;
import org.apache.gravitino.dto.stats.PartitionStatisticsDTO;
import org.apache.gravitino.dto.stats.PartitionStatisticsDropDTO;
import org.apache.gravitino.dto.stats.PartitionStatisticsUpdateDTO;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.IllegalStatisticNameException;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.PartitionStatisticsModification;
import org.apache.gravitino.stats.Statistic;
import org.apache.gravitino.stats.StatisticManager;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/metalakes/{metalake}/objects/{type}/{fullName}/statistics")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class StatisticOperations {

  private static final Logger LOG = LoggerFactory.getLogger(StatisticOperations.class);

  @Context private HttpServletRequest httpRequest;

  private final StatisticManager statisticManager;

  @Inject
  public StatisticOperations(StatisticManager statisticManager) {
    this.statisticManager = statisticManager;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-stats." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-stats", absolute = true)
  public Response listStatistics(
      @PathParam("metalake") String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName) {
    LOG.info(
        "Received list statistics request for object full name: {} type: {} in the metalake {}",
        fullName,
        type,
        metalake);
    try {

      return Utils.doAs(
          httpRequest,
          () -> {
            MetadataObject object =
                MetadataObjects.parse(
                    fullName, MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT)));
            if (object.type() != MetadataObject.Type.TABLE) {
              throw new UnsupportedOperationException(
                  "Listing statistics is only supported for tables now.");
            }

            MetadataObjectUtil.checkMetadataObject(metalake, object);

            List<Statistic> statistics = statisticManager.listStatistics(metalake, object);
            return Utils.ok(
                new StatisticListResponse(
                    DTOConverters.toDTOs(statistics.toArray(new Statistic[0]))));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleStatisticException(OperationType.LIST, fullName, metalake, e);
    }
  }

  @PUT
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "update-stats." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "update-stats", absolute = true)
  public Response updateStatistics(
      @PathParam("metalake") String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
      StatisticsUpdateRequest request) {
    try {
      LOG.info(
          "Received update statistics request for object full name: {} type: {} in the metalake {}",
          fullName,
          type,
          metalake);
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            MetadataObject object =
                MetadataObjects.parse(
                    fullName, MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT)));
            if (object.type() != MetadataObject.Type.TABLE) {
              throw new UnsupportedOperationException(
                  "Update statistics is only supported for tables now.");
            }

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

            MetadataObjectUtil.checkMetadataObject(metalake, object);

            statisticManager.updateStatistics(metalake, object, statisticMaps);
            return Utils.ok(new BaseResponse(0));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleStatisticException(
          OperationType.UPDATE, StringUtils.join(request.getUpdates().keySet(), ","), fullName, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-stats." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-stats", absolute = true)
  public Response dropStatistics(
      @PathParam("metalake") String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
      StatisticsDropRequest request) {
    try {
      LOG.info(
          "Received drop statistics request for object full name: {} type: {} in the metalake {}",
          fullName,
          type,
          metalake);
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();

            MetadataObject object =
                MetadataObjects.parse(
                    fullName, MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT)));
            if (object.type() != MetadataObject.Type.TABLE) {
              throw new UnsupportedOperationException(
                  "Dropping statistics is only supported for tables now.");
            }

            MetadataObjectUtil.checkMetadataObject(metalake, object);

            boolean dropped =
                statisticManager.dropStatistics(
                    metalake, object, Lists.newArrayList(request.getNames()));
            return Utils.ok(new DropResponse(dropped));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleStatisticException(
          OperationType.DROP, StringUtils.join(request.getNames(), ","), fullName, e);
    }
  }

  @GET
  @Path("/partitions")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-partition-stats." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-partition-stats", absolute = true)
  public Response listPartitionStatistics(
      @PathParam("metalake") String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
      @QueryParam("from") String fromPartitionName,
      @QueryParam("to") String toPartitionName,
      @QueryParam("fromInclusive") @DefaultValue("true") boolean fromInclusive,
      @QueryParam("toInclusive") @DefaultValue("false") boolean toInclusive) {

    String formattedFromPartitionName =
        getFormattedFromPartitionName(fromPartitionName, fromInclusive);
    String formattedToPartitionName = getFormattedToPartitionName(toPartitionName, toInclusive);

    LOG.info(
        "Listing partition statistics for table: {} in the metalake {} from {} to {}",
        fullName,
        metalake,
        formattedFromPartitionName,
        formattedToPartitionName);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            MetadataObject object =
                MetadataObjects.parse(
                    fullName, MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT)));
            if (object.type() != MetadataObject.Type.TABLE) {
              throw new UnsupportedOperationException(
                  "Listing partition statistics is only supported for tables now.");
            }

            if (fromPartitionName == null && toPartitionName == null) {
              throw new IllegalArgumentException(
                  "Both 'from' and 'to' parameters cannot be null at the same time.");
            }

            MetadataObjectUtil.checkMetadataObject(metalake, object);

            PartitionRange range;
            PartitionRange.BoundType fromBoundType = getFromBoundType(fromInclusive);
            PartitionRange.BoundType toBoundType = getFromBoundType(toInclusive);
            if (fromPartitionName != null && toPartitionName != null) {
              range =
                  PartitionRange.between(
                      fromPartitionName, fromBoundType, toPartitionName, toBoundType);
            } else if (fromPartitionName != null) {
              range = PartitionRange.downTo(fromPartitionName, fromBoundType);
            } else {
              range = PartitionRange.upTo(toPartitionName, toBoundType);
            }

            List<PartitionStatistics> statistics =
                statisticManager.listPartitionStatistics(metalake, object, range);

            PartitionStatisticsDTO[] partitionStatistics =
                statistics.stream()
                    .map(
                        partitionStatistic ->
                            PartitionStatisticsDTO.of(
                                partitionStatistic.partitionName(),
                                DTOConverters.toDTOs(partitionStatistic.statistics())))
                    .toArray(PartitionStatisticsDTO[]::new);

            return Utils.ok(new PartitionStatisticsListResponse(partitionStatistics));
          });
    } catch (Exception e) {
      LOG.error(
          "Error listing {},{} partition statistics for table: {} in the metalake {}.",
          formattedFromPartitionName,
          formattedToPartitionName,
          fullName,
          metalake,
          e);
      return ExceptionHandlers.handlePartitionStatsException(
          OperationType.LIST,
          formattedFromPartitionName + "," + formattedToPartitionName,
          fullName,
          e);
    }
  }

  @PUT
  @Path("/partitions")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "update-partitions-stats." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "update-partitions-stats", absolute = true)
  public Response updatePartitionStatistics(
      @PathParam("metalake") String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
      PartitionStatisticsUpdateRequest request) {
    LOG.info("Updating partition statistics for table: {} in the metalake {}", fullName, metalake);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();

            MetadataObject object =
                MetadataObjects.parse(
                    fullName, MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT)));
            if (object.type() != MetadataObject.Type.TABLE) {
              throw new UnsupportedOperationException(
                  "Updating partition statistics is only supported for tables now.");
            }

            List<PartitionStatisticsUpdateDTO> updates = request.getUpdates();
            for (PartitionStatisticsUpdateDTO update : updates) {
              update
                  .statistics()
                  .keySet()
                  .forEach(
                      statistic -> {
                        if (!statistic.startsWith(Statistic.CUSTOM_PREFIX)) {
                          // Current we only support custom statistics
                          throw new IllegalStatisticNameException(
                              "Statistic name must start with %s, but got: %s",
                              Statistic.CUSTOM_PREFIX, statistic);
                        }
                      });
            }

            MetadataObjectUtil.checkMetadataObject(metalake, object);

            statisticManager.updatePartitionStatistics(
                metalake,
                object,
                updates.stream()
                    .map(
                        update ->
                            PartitionStatisticsModification.update(
                                update.partitionName(), update.statistics()))
                    .collect(Collectors.toList()));

            return Utils.ok(new BaseResponse(0));
          });
    } catch (Exception e) {
      LOG.error(
          "Error updating partition statistics for table: {} in the metalake {}",
          fullName,
          metalake,
          e);
      String partitions =
          StringUtils.joinWith(
              ",",
              request.getUpdates().stream()
                  .map(PartitionStatisticsUpdateDTO::partitionName)
                  .collect(Collectors.toList()));
      return ExceptionHandlers.handlePartitionStatsException(
          OperationType.UPDATE, partitions, fullName, e);
    }
  }

  @POST
  @Path("/partitions")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-partitions-stats." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-partitions-stats", absolute = true)
  public Response dropPartitionStatistics(
      @PathParam("metalake") String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
      PartitionStatisticsDropRequest request) {

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            MetadataObject object =
                MetadataObjects.parse(
                    fullName, MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT)));
            if (object.type() != MetadataObject.Type.TABLE) {
              throw new UnsupportedOperationException(
                  "Dropping partition statistics is only supported for tables now.");
            }

            MetadataObjectUtil.checkMetadataObject(metalake, object);

            return Utils.ok(
                new DropResponse(
                    statisticManager.dropPartitionStatistics(
                        metalake,
                        object,
                        request.getDrops().stream()
                            .map(
                                drop ->
                                    PartitionStatisticsModification.drop(
                                        drop.partitionName(), drop.statisticNames()))
                            .collect(Collectors.toList()))));
          });
    } catch (Exception e) {
      LOG.error(
          "Error dropping partition statistics for table: {} in the metalake {}.",
          fullName,
          metalake,
          e);
      String partitions =
          StringUtils.joinWith(
              ",",
              request.getDrops().stream()
                  .map(PartitionStatisticsDropDTO::partitionName)
                  .collect(Collectors.toList()));
      return ExceptionHandlers.handlePartitionStatsException(
          OperationType.DROP, partitions, fullName, e);
    }
  }

  @VisibleForTesting
  static PartitionRange.BoundType getFromBoundType(boolean inclusive) {
    return inclusive ? PartitionRange.BoundType.CLOSED : PartitionRange.BoundType.OPEN;
  }

  private static String getFormattedFromPartitionName(
      String fromPartitionName, boolean fromInclusive) {
    if (fromPartitionName == null) {
      return "(-INF";
    } else {
      if (fromInclusive) {
        return "[" + fromPartitionName;
      } else {
        return "(" + fromPartitionName;
      }
    }
  }

  private static String getFormattedToPartitionName(String toPartitionName, boolean toInclusive) {
    if (toPartitionName == null) {
      return "INF)";
    } else {
      if (toInclusive) {
        return toPartitionName + "]";
      } else {
        return toPartitionName + ")";
      }
    }
  }
}
