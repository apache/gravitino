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
package org.apache.gravitino.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.dto.requests.PartitionStatisticsDropRequest;
import org.apache.gravitino.dto.requests.PartitionStatisticsUpdateRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.PartitionStatisticsListResponse;
import org.apache.gravitino.dto.stats.PartitionStatisticsDropDTO;
import org.apache.gravitino.dto.stats.PartitionStatisticsUpdateDTO;
import org.apache.gravitino.exceptions.UnmodifiableStatisticException;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.PartitionStatisticsDrop;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.SupportsPartitionStatistics;

/**
 * The implementation of {@link SupportsPartitionStatistics}. This interface will be composited into
 * table to provide partition statistics operations for metadata objects.
 */
class MetadataObjectPartitionStatisticsOperations implements SupportsPartitionStatistics {

  private static final String FROM = "from";
  private static final String FROM_INCLUSIVE = "fromInclusive";
  private static final String TO = "to";
  private static final String TO_INCLUSIVE = "toInclusive";
  private final RESTClient restClient;
  private final String statisticsRequestPath;

  MetadataObjectPartitionStatisticsOperations(
      String metalakeName, MetadataObject metadataObject, RESTClient restClient) {
    this.restClient = restClient;
    this.statisticsRequestPath =
        String.format(
            "api/metalakes/%s/objects/%s/%s/statistics/partitions",
            RESTUtils.encodeString(metalakeName),
            metadataObject.type().name().toLowerCase(Locale.ROOT),
            RESTUtils.encodeString(metadataObject.fullName()));
  }

  @Override
  public List<PartitionStatistics> listPartitionStatistics(PartitionRange range) {
    Map<String, String> queryParams = Maps.newHashMap();
    range.lowerPartitionName().ifPresent(from -> queryParams.put(FROM, from));
    range
        .lowerBoundType()
        .ifPresent(
            boundType ->
                queryParams.put(
                    FROM_INCLUSIVE, String.valueOf(boundType == PartitionRange.BoundType.CLOSED)));

    range.upperPartitionName().ifPresent(to -> queryParams.put(TO, to));
    range
        .upperBoundType()
        .ifPresent(
            boundType ->
                queryParams.put(
                    TO_INCLUSIVE, String.valueOf(boundType == PartitionRange.BoundType.CLOSED)));

    PartitionStatisticsListResponse response =
        restClient.get(
            statisticsRequestPath,
            queryParams,
            PartitionStatisticsListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.statisticsErrorHandler());
    response.validate();

    return Lists.newArrayList(response.getPartitionStatistics());
  }

  @Override
  public void updatePartitionStatistics(List<PartitionStatisticsUpdate> statisticsToUpdate)
      throws UnmodifiableStatisticException {
    List<PartitionStatisticsUpdateDTO> updates =
        statisticsToUpdate.stream()
            .map(
                update ->
                    PartitionStatisticsUpdateDTO.of(update.partitionName(), update.statistics()))
            .collect(Collectors.toList());
    PartitionStatisticsUpdateRequest request = new PartitionStatisticsUpdateRequest(updates);
    request.validate();

    BaseResponse response =
        restClient.put(
            statisticsRequestPath,
            request,
            BaseResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.statisticsErrorHandler());
    response.validate();
  }

  @Override
  public boolean dropPartitionStatistics(List<PartitionStatisticsDrop> statisticsToDrop)
      throws UnmodifiableStatisticException {
    PartitionStatisticsDropRequest request =
        new PartitionStatisticsDropRequest(
            statisticsToDrop.stream()
                .map(
                    drop ->
                        PartitionStatisticsDropDTO.of(
                            drop.partitionName(), (drop.statisticNames())))
                .collect(Collectors.toList()));
    request.validate();

    DropResponse response =
        restClient.post(
            statisticsRequestPath,
            request,
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.statisticsErrorHandler());
    response.validate();

    return response.dropped();
  }
}
