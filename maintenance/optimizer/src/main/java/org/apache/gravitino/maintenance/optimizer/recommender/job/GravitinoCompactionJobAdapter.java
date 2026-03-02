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

package org.apache.gravitino.maintenance.optimizer.recommender.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.common.util.IdentifierUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionJobContext;

public class GravitinoCompactionJobAdapter implements GravitinoJobAdapter {

  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

  @Override
  public Map<String, String> jobConfig(JobExecutionContext jobExecutionContext) {
    Preconditions.checkArgument(
        jobExecutionContext instanceof CompactionJobContext,
        "jobExecutionContext must be CompactionJobExecutionContext");
    CompactionJobContext jobContext = (CompactionJobContext) jobExecutionContext;
    return ImmutableMap.of(
        "table_identifier", getTableName(jobContext),
        "where_clause", getWhereClause(jobContext),
        "sort_order", "",
        "strategy", "binpack",
        "options", getOptions(jobContext));
  }

  private String getTableName(CompactionJobContext jobContext) {
    return IdentifierUtils.removeCatalogFromIdentifier(jobContext.nameIdentifier()).toString();
  }

  private String getWhereClause(CompactionJobContext jobContext) {
    if (jobContext.getPartitions().isEmpty()) {
      return "";
    }
    // generate where clause from jobContext.partitionNames()
    // 1. get partition column type
    // 2. generate partition filter name, like day(xxx)
    // 3. generate partition filter value, like '2023-10-01', TIMESTAMP 'xxx'
    return PartitionUtils.getWhereClauseForPartitions(
        jobContext.getPartitions(), jobContext.getColumns(), jobContext.getPartitioning());
  }

  private String getOptions(JobExecutionContext jobExecutionContext) {
    Map<String, String> map = jobExecutionContext.jobOptions();
    return convertMapToJson(map);
  }

  private static String convertMapToJson(Map<String, ?> map) {
    if (map == null || map.isEmpty()) {
      return "{}";
    }
    try {
      return MAPPER.writeValueAsString(new TreeMap<>(map));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize options to JSON", e);
    }
  }
}
