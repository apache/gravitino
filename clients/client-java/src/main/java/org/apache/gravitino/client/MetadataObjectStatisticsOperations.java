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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.dto.requests.StatisticsUpdateRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.StatisticListResponse;
import org.apache.gravitino.dto.stats.StatisticValueDTO;
import org.apache.gravitino.exceptions.IllegalStatisticNameException;
import org.apache.gravitino.exceptions.UnmodifiableStatisticException;
import org.apache.gravitino.stats.Statistic;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.SupportsStatistics;

/**
 * The implementation of {@link SupportsStatistics}. This interface will be composited into table,
 * partition and fileset to provide statistics operations for these metadata objects.
 */
class MetadataObjectStatisticsOperations implements SupportsStatistics {

  private final RESTClient restClient;
  private final String statisticsRequestPath;

  MetadataObjectStatisticsOperations(
      String metalakeName,
      String catalogName,
      MetadataObject metadataObject,
      RESTClient restClient) {
    this.restClient = restClient;

    // Build the REST API path based on the metadata object type
    String fullName = metadataObject.fullName();

    // For different object types, the path pattern is different
    switch (metadataObject.type()) {
      case TABLE:
        // For table:
        // api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}/statistics
        String[] parts = fullName.split("\\.");
        Preconditions.checkArgument(parts.length == 3, "Invalid table full name: " + fullName);
        // fullName format is catalog.schema.table, we need schema and table
        this.statisticsRequestPath =
            String.format(
                "api/metalakes/%s/catalogs/%s/schemas/%s/tables/%s/statistics",
                metalakeName, catalogName, parts[1], parts[2]);
        break;
      case FILESET:
        // For fileset:
        // api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/filesets/{fileset}/statistics
        parts = fullName.split("\\.");
        Preconditions.checkArgument(parts.length == 3, "Invalid fileset full name: " + fullName);
        // fullName format is catalog.schema.fileset, we need schema and fileset
        this.statisticsRequestPath =
            String.format(
                "api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/statistics",
                metalakeName, catalogName, parts[1], parts[2]);
        break;
      default:
        throw new IllegalArgumentException(
            "Statistics are not supported for object type: " + metadataObject.type());
    }
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
  public List<Statistic> updateStatistics(Map<String, StatisticValue<?>> statistics)
      throws UnmodifiableStatisticException, IllegalStatisticNameException {
    Preconditions.checkArgument(
        statistics != null && !statistics.isEmpty(), "Statistics map must not be null or empty");

    // Convert StatisticValue to StatisticValueDTO
    Map<String, StatisticValueDTO<?>> statisticDTOs =
        statistics.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> DTOConverters.toStatisticValueDTO(entry.getValue())));

    StatisticsUpdateRequest request =
        StatisticsUpdateRequest.builder().withStatistics(statisticDTOs).build();
    request.validate();

    StatisticListResponse resp =
        restClient.post(
            statisticsRequestPath,
            request,
            StatisticListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.statisticsErrorHandler());

    resp.validate();
    return Arrays.asList(resp.getStatistics());
  }

  @Override
  public boolean dropStatistics(List<String> statistics) throws UnmodifiableStatisticException {
    Preconditions.checkArgument(
        statistics != null && !statistics.isEmpty(), "Statistics list must not be null or empty");

    // Convert the list of statistics to a query parameter
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("statistics", String.join(",", statistics));

    DropResponse resp =
        restClient.delete(
            statisticsRequestPath,
            queryParams,
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.statisticsErrorHandler());

    resp.validate();
    return resp.dropped();
  }
}
