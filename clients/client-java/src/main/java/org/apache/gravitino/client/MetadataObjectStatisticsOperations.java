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

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.dto.requests.StatisticsDropRequest;
import org.apache.gravitino.dto.requests.StatisticsUpdateRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.StatisticListResponse;
import org.apache.gravitino.exceptions.IllegalStatisticNameException;
import org.apache.gravitino.exceptions.UnmodifiableStatisticException;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.stats.Statistic;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.SupportsStatistics;

/**
 * The implementation of {@link SupportsStatistics}. This interface will be composited into table to
 * provide statistics operations for metadata objects.
 */
class MetadataObjectStatisticsOperations implements SupportsStatistics {

  private final RESTClient restClient;
  private final String statisticsRequestPath;

  MetadataObjectStatisticsOperations(
      String metalakeName, MetadataObject metadataObject, RESTClient restClient) {
    this.restClient = restClient;
    this.statisticsRequestPath =
        String.format(
            "api/metalakes/%s/objects/%s/%s/statistics",
            RESTUtils.encodeString(metalakeName),
            metadataObject.type().name().toLowerCase(Locale.ROOT),
            RESTUtils.encodeString(metadataObject.fullName()));
  }

  @Override
  public List<Statistic> listStatistics() {
    StatisticListResponse resp =
        restClient.get(
            statisticsRequestPath,
            StatisticListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.statisticsErrorHandler());

    resp.validate();

    return Arrays.asList(resp.getStatistics());
  }

  @Override
  public void updateStatistics(Map<String, StatisticValue<?>> statistics)
      throws UnmodifiableStatisticException, IllegalStatisticNameException {
    Preconditions.checkArgument(
        statistics != null && !statistics.isEmpty(), "Statistics map must not be null or empty");

    StatisticsUpdateRequest request = StatisticsUpdateRequest.builder().updates(statistics).build();
    request.validate();

    BaseResponse resp =
        restClient.put(
            statisticsRequestPath,
            request,
            BaseResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.statisticsErrorHandler());

    resp.validate();
  }

  @Override
  public boolean dropStatistics(List<String> statistics) throws UnmodifiableStatisticException {
    StatisticsDropRequest request = new StatisticsDropRequest(statistics.toArray(new String[0]));
    request.validate();

    DropResponse resp =
        restClient.post(
            statisticsRequestPath,
            request,
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.statisticsErrorHandler());

    resp.validate();

    return resp.dropped();
  }
}
