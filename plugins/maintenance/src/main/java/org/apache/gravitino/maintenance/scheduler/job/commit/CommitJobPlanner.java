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

import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.maintenance.MaintenanceActionManager;
import org.apache.gravitino.maintenance.TableMaintenanceConfig;
import org.apache.gravitino.maintenance.policy.MaintenanceMinIntervalConfigs;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpecFactory;
import org.apache.gravitino.maintenance.scheduler.MaintenanceSchedulerPools;
import org.apache.gravitino.maintenance.scheduler.MaintenanceTaskIds;
import org.apache.gravitino.maintenance.scheduler.TableCandidate;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceAuthConsistency;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceJobTemplates;
import org.apache.gravitino.maintenance.scheduler.policy.expand.NearestMaintenancePolicyResolver;
import org.apache.gravitino.maintenance.scheduler.policy.expand.NearestMaintenancePolicyResolver.Decision;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.policy.ExecutablePolicyContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.storage.relational.service.PolicyMetaService;
import org.apache.gravitino.storage.relational.service.TableMetaService;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plans a {@code commit-job}: resolve the table, skip if already queued, collect the onCommit
 * chain, drop cool-down members, check auth, and build {@link MaintenanceJobSpec}.
 */
public class CommitJobPlanner {

  private static final Logger LOG = LoggerFactory.getLogger(CommitJobPlanner.class);

  private static final List<Policy.BuiltInType> CHAIN_ORDER =
      List.of(
          Policy.BuiltInType.ICEBERG_COMPACTION,
          Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS,
          Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION);

  private final Config gravitinoConfig;
  private final MaintenanceActionManager actionManager;
  private final MaintenanceJobSpecFactory specFactory;

  /**
   * @param gravitinoConfig server config for auth overlay and minInterval fallbacks
   */
  public CommitJobPlanner(Config gravitinoConfig) {
    this.gravitinoConfig = Preconditions.checkNotNull(gravitinoConfig);
    this.actionManager =
        new MaintenanceActionManager(
            TableMaintenanceActionMetaService.getInstance(),
            TableMaintenanceConfig.from(gravitinoConfig).claimTimeoutMs());
    this.specFactory = new MaintenanceJobSpecFactory(gravitinoConfig);
  }

  /**
   * @param client commit-job scheduler client (used to see if a row already exists)
   * @param identifier {@code metalake.catalog.schema.table} from the commit event
   * @return spec to schedule, or empty when there is nothing to schedule
   */
  public Optional<MaintenanceJobSpec> plan(SchedulerClient client, NameIdentifier identifier) {
    if (identifier == null) {
      return Optional.empty();
    }
    try {
      TableEntity table = TableMetaService.getInstance().getTableByIdentifier(identifier);
      long tableId = table.id();
      String instanceId = MaintenanceTaskIds.forCommitTable(tableId);
      if (client
          .getScheduledExecution(
              TaskInstanceId.of(MaintenanceSchedulerPools.COMMIT_JOB, instanceId))
          .isPresent()) {
        LOG.debug("commit-job already queued for table {} ({})", tableId, identifier);
        return Optional.empty();
      }

      Namespace namespace = identifier.namespace();
      if (namespace == null || namespace.length() < 3 || identifier.name() == null) {
        LOG.info("Skipping commit-job for table {}: incomplete identifier {}", tableId, identifier);
        return Optional.empty();
      }
      for (int i = 0; i < namespace.length(); i++) {
        if (namespace.level(i) == null || namespace.level(i).isBlank()) {
          LOG.info("Skipping commit-job for table {}: blank namespace on {}", tableId, identifier);
          return Optional.empty();
        }
      }

      MetadataObject tableObject =
          NameIdentifierUtil.toMetadataObject(identifier, Entity.EntityType.TABLE);
      String metalake = namespace.level(0);

      NearestMaintenancePolicyResolver nearest =
          new NearestMaintenancePolicyResolver(GravitinoEnv.getInstance().entityStore());
      List<PolicyEntity> chain = new ArrayList<>();
      for (Policy.BuiltInType type : CHAIN_ORDER) {
        Decision decision = nearest.resolve(metalake, tableObject, type);
        if (decision.kind() == Decision.Kind.CONFLICT) {
          LOG.error(
              "Skip commit-job schedule for table {}: ambiguous {} policies {} on nearest level",
              tableId,
              type,
              decision.conflictingPolicyIds());
          return Optional.empty();
        }
        Optional<Long> winnerId = decision.winnerPolicyId();
        if (winnerId.isEmpty()) {
          continue;
        }
        PolicyEntity policy = PolicyMetaService.getInstance().getPolicyById(winnerId.get());
        if (policy == null || !policy.enabled()) {
          continue;
        }
        PolicyContent content = policy.content();
        Boolean commitTrigger =
            content instanceof ExecutablePolicyContent
                ? ((ExecutablePolicyContent) content).executionSpec().commitTrigger()
                : null;
        if (!Boolean.TRUE.equals(commitTrigger)) {
          continue;
        }
        if (MaintenanceJobTemplates.maintenanceType(policy.policyType()).isEmpty()) {
          continue;
        }
        chain.add(policy);
      }
      if (chain.isEmpty()) {
        LOG.debug("Skipping commit-job schedule for table {}: empty onCommit chain", tableId);
        return Optional.empty();
      }

      long nowMs = System.currentTimeMillis();
      List<PolicyEntity> due = new ArrayList<>();
      for (PolicyEntity policy : chain) {
        Long contentMinInterval = null;
        if (policy.content() instanceof ExecutablePolicyContent) {
          contentMinInterval =
              ((ExecutablePolicyContent) policy.content()).executionSpec().minIntervalMs();
        }
        long minIntervalMs =
            MaintenanceMinIntervalConfigs.resolve(
                gravitinoConfig, policy.policyType(), contentMinInterval);
        if (actionManager.isCoolingDown(tableId, policy.id(), minIntervalMs, nowMs)) {
          LOG.debug(
              "Dropping type {} policy {} from commit-job schedule for table {} (within minIntervalMs={})",
              policy.policyType(),
              policy.id(),
              tableId,
              minIntervalMs);
          continue;
        }
        due.add(policy);
      }
      if (due.isEmpty()) {
        LOG.debug(
            "Skipping commit-job schedule for table {}: all onCommit types within minIntervalMs",
            tableId);
        return Optional.empty();
      }

      MaintenanceAuthConsistency.requireSameAuth(due, gravitinoConfig);
      return Optional.of(
          specFactory.forCommit(
              List.of(
                  new TableCandidate(
                      tableId,
                      MaintenanceJobSpecFactory.catalogName(identifier),
                      MaintenanceJobSpecFactory.sparkTableIdentifier(identifier),
                      due))));
    } catch (NoSuchEntityException e) {
      LOG.debug("No Gravitino table for IRC update {}: {}", identifier, e.getMessage());
      return Optional.empty();
    } catch (IllegalArgumentException e) {
      LOG.error("Skipping commit-job schedule for {}: {}", identifier, e.getMessage());
      return Optional.empty();
    } catch (RuntimeException e) {
      LOG.warn("Failed to plan commit-job for IRC update {}: {}", identifier, e.toString());
      return Optional.empty();
    }
  }
}
