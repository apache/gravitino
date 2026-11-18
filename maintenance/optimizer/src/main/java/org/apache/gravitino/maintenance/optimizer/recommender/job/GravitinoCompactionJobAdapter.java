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
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.common.util.IdentifierUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionJobContext;
import org.apache.gravitino.policy.IcebergDataCompactionContent;

public class GravitinoCompactionJobAdapter implements GravitinoJobAdapter {

  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

  @Override
  public Map<String, String> jobConfig(JobExecutionContext jobExecutionContext) {
    return jobConfig(jobExecutionContext, Map.of());
  }

  /**
   * Builds compaction jobConf. {@code where_clause} qualifies Iceberg system functions with the
   * same Spark {@code catalog_name} the CALL uses (submitter config), not the Gravitino catalog
   * segment of the table identifier when those differ.
   */
  @Override
  public Map<String, String> jobConfig(
      JobExecutionContext jobExecutionContext, Map<String, String> submitterConfigs) {
    Preconditions.checkArgument(
        jobExecutionContext instanceof CompactionJobContext,
        "jobExecutionContext must be CompactionJobExecutionContext");
    CompactionJobContext jobContext = (CompactionJobContext) jobExecutionContext;
    String rewriteStrategy =
        jobContext.getRewriteStrategy() == null
            ? IcebergDataCompactionContent.DEFAULT_REWRITE_STRATEGY
            : jobContext.getRewriteStrategy();
    String sortOrder =
        jobContext.getSortOrder() == null
            ? IcebergDataCompactionContent.DEFAULT_SORT_ORDER
            : jobContext.getSortOrder();
    return ImmutableMap.of(
        "table_identifier", getTableName(jobContext),
        "where_clause", getWhereClause(jobContext, submitterConfigs),
        "sort_order", sortOrder,
        "strategy", rewriteStrategy,
        "options", getOptions(jobContext));
  }

  private String getTableName(CompactionJobContext jobContext) {
    return IdentifierUtils.removeCatalogFromIdentifier(jobContext.nameIdentifier()).toString();
  }

  private String getWhereClause(
      CompactionJobContext jobContext, Map<String, String> submitterConfigs) {
    if (jobContext.getPartitions().isEmpty()) {
      return "";
    }
    // Same Spark catalog alias as CALL (jobSubmitterConfig.catalog_name); fall back to the
    // Gravitino table catalog when unset.
    String sparkCatalog = submitterConfigs == null ? null : submitterConfigs.get("catalog_name");
    if (StringUtils.isBlank(sparkCatalog)) {
      sparkCatalog = IdentifierUtils.getCatalogNameFromTableIdentifier(jobContext.nameIdentifier());
    } else {
      sparkCatalog = sparkCatalog.trim();
    }
    return PartitionUtils.getWhereClauseForPartitions(
        jobContext.getPartitions(),
        jobContext.getColumns(),
        jobContext.getPartitioning(),
        sparkCatalog);
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
