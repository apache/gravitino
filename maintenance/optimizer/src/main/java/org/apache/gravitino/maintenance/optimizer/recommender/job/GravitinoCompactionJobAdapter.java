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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.IdentifierUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionJobContext;
import org.apache.gravitino.policy.IcebergDataCompactionContent;

public class GravitinoCompactionJobAdapter implements GravitinoJobAdapter {

  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

  /**
   * Builds compaction jobConf. {@code where_clause} qualifies Iceberg system functions with the
   * same Spark {@code catalog_name} the CALL uses (submitter config), not the Gravitino catalog
   * segment of the table identifier when those differ.
   */
  @Override
  public Map<String, String> jobConfig(
      JobExecutionContext jobExecutionContext,
      Map<String, String> submitterConfigs,
      String schemaSeparator) {
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
    if (submitterConfigs == null) {
      submitterConfigs = Collections.emptyMap();
    }
    String catalogName = sparkCatalogName(submitterConfigs, jobContext);
    Map<String, String> config = new LinkedHashMap<>();
    config.putAll(
        IcebergSparkConfigUtils.resolveTemplateSparkPlaceholders(
            catalogName,
            IcebergSparkConfigUtils.parseFlatJsonMap(
                submitterConfigs.get("spark_conf"), "spark-conf"),
            submitterConfigs));
    config.put(
        "table_identifier",
        IdentifierUtils.toSparkTableIdentifier(jobContext.nameIdentifier(), schemaSeparator));
    config.put("where_clause", getWhereClause(jobContext, submitterConfigs));
    config.put("sort_order", sortOrder);
    config.put("strategy", rewriteStrategy);
    config.put("options", getOptions(jobContext));
    return Map.copyOf(config);
  }

  private static String sparkCatalogName(
      Map<String, String> submitterConfigs, CompactionJobContext jobContext) {
    String sparkCatalog = StringUtils.trimToNull(submitterConfigs.get("catalog_name"));
    if (sparkCatalog != null) {
      return sparkCatalog;
    }
    return IdentifierUtils.getCatalogNameFromTableIdentifier(jobContext.nameIdentifier());
  }

  private String getWhereClause(
      CompactionJobContext jobContext, Map<String, String> submitterConfigs) {
    if (jobContext.getPartitions().isEmpty()) {
      return "";
    }
    // Same Spark catalog alias as CALL (jobSubmitterConfig.catalog_name); fall back to the
    // Gravitino table catalog when unset.
    String sparkCatalog = StringUtils.trimToNull(submitterConfigs.get("catalog_name"));
    if (sparkCatalog == null) {
      sparkCatalog = IdentifierUtils.getCatalogNameFromTableIdentifier(jobContext.nameIdentifier());
    }
    return PartitionUtils.getWhereClauseForPartitions(
        jobContext.getPartitions(),
        jobContext.getColumns(),
        jobContext.getPartitioning(),
        sparkCatalog);
  }

  private String getOptions(JobExecutionContext jobExecutionContext) {
    try {
      return MAPPER.writeValueAsString(new TreeMap<>(jobExecutionContext.jobOptions()));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize job options", e);
    }
  }
}
