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
package org.apache.gravitino.maintenance.scheduler.job.commit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec.Task;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpecFactory;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpecFactory.JobConfParts;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceJobConfBuilder;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceTableRefs.ResolvedTableRef;

/**
 * Builds enqueue-time {@link MaintenanceJobSpec} values for the commit-job path (keeps commit →
 * scheduler dependency one-way; no scheduler → commit package cycle).
 *
 * <p>Each onCommit chain member becomes one {@link Task} with its own {@code parameters}. Shared
 * {@code updater_options} come from the first policy; {@code spark_conf} is merged across members
 * (first-wins keys, max resource-shaped values).
 */
final class CommitJobSpecFactory {

  private final MaintenanceJobConfBuilder jobConfBuilder;

  /**
   * @param gravitinoConfig server config for auth overlay and gravitino URI
   */
  CommitJobSpecFactory(Config gravitinoConfig) {
    this.jobConfBuilder =
        new MaintenanceJobConfBuilder(Preconditions.checkNotNull(gravitinoConfig));
  }

  /**
   * Commit-chain spec for one table. Caller must already have filtered cooldown and verified auth
   * consistency.
   *
   * @param chain non-empty onCommit members in order
   * @param ref resolved Spark table ref
   * @return spec with one task per chain member
   */
  MaintenanceJobSpec forCommit(List<CommitChainMember> chain, ResolvedTableRef ref) {
    Preconditions.checkArgument(chain != null && !chain.isEmpty(), "chain must not be empty");
    Preconditions.checkNotNull(ref, "ref");

    Map<String, String> firstBuilt = jobConfBuilder.build(chain.get(0).policy(), ref.catalogName());
    MaintenanceJobConfBuilder.foldPlaceholdersIntoSparkConf(firstBuilt, ref.catalogName());
    JobConfParts first = MaintenanceJobSpecFactory.splitJobConfParts(firstBuilt);
    String updaterOptions = first.updaterOptions;
    Map<String, String> sparkConf =
        new LinkedHashMap<>(
            IcebergSparkConfigUtils.parseFlatJsonMap(nullToEmpty(first.sparkConf), "spark_conf"));

    List<Task> tasks = new ArrayList<>(chain.size());
    for (int i = 0; i < chain.size(); i++) {
      CommitChainMember member = chain.get(i);
      JobConfParts parts;
      if (i == 0) {
        parts = first;
      } else {
        Map<String, String> built = jobConfBuilder.build(member.policy(), ref.catalogName());
        MaintenanceJobConfBuilder.foldPlaceholdersIntoSparkConf(built, ref.catalogName());
        parts = MaintenanceJobSpecFactory.splitJobConfParts(built);
        MaintenanceJobSpecFactory.mergeSparkConfMaps(sparkConf, parts.sparkConf);
      }
      Map<String, String> parameters = new LinkedHashMap<>(parts.parameters);
      parameters.put("catalog_name", ref.catalogName());
      parameters.put("table_identifier", ref.tableIdentifier());
      tasks.add(
          new Task(
              ref.tableId(), member.policy().id(), member.maintenanceType(), null, parameters));
    }

    String sparkConfJson = sparkConf.isEmpty() ? null : writeJson(sparkConf, "spark_conf");
    return new MaintenanceJobSpec(tasks, updaterOptions, sparkConfJson);
  }

  private static String writeJson(Object value, String field) {
    try {
      return JsonUtils.anyFieldMapper().writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize " + field, e);
    }
  }

  private static String nullToEmpty(@Nullable String value) {
    return value == null ? "" : value;
  }
}
