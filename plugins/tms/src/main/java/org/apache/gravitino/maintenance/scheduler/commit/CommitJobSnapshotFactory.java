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
package org.apache.gravitino.maintenance.scheduler.commit;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.Config;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshot;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshot.ChainStep;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshot.TableTarget;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshotFactory;
import org.apache.gravitino.maintenance.scheduler.cron.MaintenanceJobConfBuilder;
import org.apache.gravitino.maintenance.scheduler.cron.MaintenanceJobTemplates;
import org.apache.gravitino.maintenance.scheduler.cron.MaintenanceTableRefs.ResolvedTableRef;

/**
 * Builds enqueue-time {@link MaintenanceJobSnapshot} values for the commit-job path (keeps commit →
 * scheduler dependency one-way; no scheduler → commit package cycle).
 */
final class CommitJobSnapshotFactory {

  private final MaintenanceJobConfBuilder jobConfBuilder;

  /**
   * @param gravitinoConfig server config for auth overlay and gravitino URI
   */
  CommitJobSnapshotFactory(Config gravitinoConfig) {
    this.jobConfBuilder =
        new MaintenanceJobConfBuilder(Preconditions.checkNotNull(gravitinoConfig));
  }

  /**
   * Commit-chain snapshot for one table. Caller must already have filtered cooldown and verified
   * auth consistency.
   *
   * @param chain non-empty onCommit members in order
   * @param ref resolved Spark table ref
   * @return snapshot with merged {@code jobConf} including {@code maintenance_types}
   */
  MaintenanceJobSnapshot forCommit(List<CommitChainMember> chain, ResolvedTableRef ref) {
    Preconditions.checkArgument(chain != null && !chain.isEmpty(), "chain must not be empty");
    Preconditions.checkNotNull(ref, "ref");

    CommitChainMember first = chain.get(0);
    String metalake = first.policy().namespace().level(0);
    Map<String, String> jobConf = buildChainJobConf(chain, ref);
    List<TableTarget> targets = List.of(MaintenanceJobSnapshotFactory.toTarget(metalake, ref));
    List<ChainStep> steps =
        chain.stream()
            .map(m -> new ChainStep(m.policy().id(), m.maintenanceType()))
            .collect(Collectors.toList());
    return new MaintenanceJobSnapshot(metalake, targets, steps, jobConf);
  }

  private Map<String, String> buildChainJobConf(
      List<CommitChainMember> chain, ResolvedTableRef ref) {
    CommitChainMember first = chain.get(0);
    Map<String, String> jobConf = jobConfBuilder.build(first.policy(), ref.catalogName());
    for (int i = 1; i < chain.size(); i++) {
      Map<String, String> other = jobConfBuilder.build(chain.get(i).policy(), ref.catalogName());
      MaintenanceJobSnapshotFactory.mergeResourceKeys(jobConf, other);
    }
    List<String> types =
        chain.stream().map(CommitChainMember::maintenanceType).collect(Collectors.toList());
    jobConf.put(MaintenanceJobTemplates.MAINTENANCE_TYPES_KEY, String.join(",", types));
    jobConf.put(MaintenanceJobTemplates.MAINTENANCE_TYPE_KEY, first.maintenanceType());
    jobConf.put("catalog_name", ref.catalogName());
    jobConf.put("table_identifier", ref.tableIdentifier());
    jobConf.put(MaintenanceJobTemplates.TMS_TABLE_ID_KEY, Long.toString(ref.tableId()));
    jobConf.put(MaintenanceJobTemplates.TMS_POLICY_ID_KEY, Long.toString(first.policy().id()));
    return jobConf;
  }
}
