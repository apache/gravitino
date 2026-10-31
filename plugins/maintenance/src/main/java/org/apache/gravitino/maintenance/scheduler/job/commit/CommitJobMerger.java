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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.maintenance.MaintenanceActionManager;
import org.apache.gravitino.maintenance.TableMaintenanceConfig;
import org.apache.gravitino.maintenance.policy.MaintenanceMinIntervalConfigs;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpecFactory;
import org.apache.gravitino.maintenance.scheduler.ResolvedTableRef;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceAuthConsistency;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceJobTemplates;
import org.apache.gravitino.maintenance.scheduler.policy.expand.NearestMaintenancePolicyResolver;
import org.apache.gravitino.maintenance.scheduler.policy.expand.NearestMaintenancePolicyResolver.Decision;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.ExecutablePolicyContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.storage.relational.service.PolicyMetaService;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the onCommit maintenance chain for a table and turns it into an enqueue-time {@link
 * MaintenanceJobSpec}: compaction → rewrite manifests → snapshot expiry, dropping members still in
 * cool-down, then requiring matching auth.
 */
public class CommitJobMerger {

  private static final Logger LOG = LoggerFactory.getLogger(CommitJobMerger.class);

  /** Fixed commit-chain order. */
  private static final List<Policy.BuiltInType> CHAIN_ORDER =
      List.of(
          Policy.BuiltInType.ICEBERG_COMPACTION,
          Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS,
          Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION);

  private final Config gravitinoConfig;
  private final MaintenanceActionManager actionManager;
  private final MaintenanceJobSpecFactory specFactory;
  private final BiFunction<Long, NameIdentifier, Optional<Chain>> collectChain;

  /**
   * @param gravitinoConfig server config for auth overlay and minInterval fallbacks
   */
  public CommitJobMerger(Config gravitinoConfig) {
    this(
        gravitinoConfig,
        new MaintenanceActionManager(
            TableMaintenanceActionMetaService.getInstance(),
            TableMaintenanceConfig.from(gravitinoConfig).claimTimeoutMs()),
        nearestChainCollector());
  }

  /**
   * @param gravitinoConfig server config
   * @param actionManager cooldown checks
   * @param collectChain returns onCommit policies plus table ref for a table
   */
  CommitJobMerger(
      Config gravitinoConfig,
      MaintenanceActionManager actionManager,
      BiFunction<Long, NameIdentifier, Optional<Chain>> collectChain) {
    this.gravitinoConfig = Preconditions.checkNotNull(gravitinoConfig);
    this.actionManager = Preconditions.checkNotNull(actionManager);
    this.collectChain = Preconditions.checkNotNull(collectChain);
    this.specFactory = new MaintenanceJobSpecFactory(gravitinoConfig);
  }

  /**
   * Collects the onCommit chain, drops cool-down members, checks auth, and builds the enqueue-time
   * spec.
   *
   * @param tableId table entity id
   * @param identifier {@code metalake.catalog.schema.table} from the commit event
   * @return empty when there is nothing to enqueue
   */
  public Optional<MaintenanceJobSpec> merge(long tableId, NameIdentifier identifier) {
    Optional<Chain> collected = collectChain.apply(tableId, identifier);
    if (collected.isEmpty()) {
      return Optional.empty();
    }
    List<PolicyEntity> policies = collected.get().policies;
    if (policies.isEmpty()) {
      LOG.debug("Skipping commit-job enqueue for table {}: empty onCommit chain", tableId);
      return Optional.empty();
    }

    long nowMs = System.currentTimeMillis();
    List<PolicyEntity> due = new ArrayList<>();
    for (PolicyEntity policy : policies) {
      long minIntervalMs =
          MaintenanceMinIntervalConfigs.resolve(
              gravitinoConfig, policy.policyType(), policyMinIntervalMs(policy.content()));
      if (actionManager.isCoolingDown(tableId, policy.id(), minIntervalMs, nowMs)) {
        LOG.debug(
            "Dropping type {} policy {} from commit-job enqueue for table {} (within minIntervalMs={})",
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
          "Skipping commit-job enqueue for table {}: all onCommit types within minIntervalMs",
          tableId);
      return Optional.empty();
    }

    try {
      MaintenanceAuthConsistency.requireSameAuth(due, gravitinoConfig);
    } catch (IllegalArgumentException e) {
      LOG.error("Skipping commit-job enqueue for table {}: {}", tableId, e.getMessage());
      return Optional.empty();
    }

    try {
      return Optional.of(specFactory.forCommit(due, collected.get().tableRef));
    } catch (RuntimeException e) {
      LOG.warn(
          "Skipping commit-job enqueue for table {}: failed to build jobConf spec: {}",
          tableId,
          e.toString());
      return Optional.empty();
    }
  }

  private static BiFunction<Long, NameIdentifier, Optional<Chain>> nearestChainCollector() {
    Function<Long, Optional<PolicyEntity>> policyLoader =
        id -> Optional.ofNullable(PolicyMetaService.getInstance().getPolicyById(id));
    Supplier<EntityStore> entityStore = () -> GravitinoEnv.getInstance().entityStore();
    return (tableId, identifier) ->
        collectNearestChain(tableId, identifier, policyLoader, entityStore);
  }

  private static Optional<Chain> collectNearestChain(
      long tableId,
      NameIdentifier identifier,
      Function<Long, Optional<PolicyEntity>> policyLoader,
      Supplier<EntityStore> entityStore) {
    if (!usableTableIdentifier(identifier)) {
      LOG.info(
          "Skipping commit chain for table {}: missing catalog and schema on {}",
          tableId,
          identifier);
      return Optional.empty();
    }
    MetadataObject tableObject;
    try {
      tableObject = NameIdentifierUtil.toMetadataObject(identifier, Entity.EntityType.TABLE);
    } catch (IllegalArgumentException e) {
      LOG.info("Skipping commit chain for table {}: {}", tableId, e.getMessage());
      return Optional.empty();
    }
    String metalake = identifier.namespace().level(0);
    ResolvedTableRef tableRef =
        new ResolvedTableRef(
            tableId, identifier.namespace().level(1), sparkTableIdentifier(identifier));
    NearestMaintenancePolicyResolver nearest =
        new NearestMaintenancePolicyResolver(entityStore.get());
    List<PolicyEntity> chain = new ArrayList<>();
    for (Policy.BuiltInType type : CHAIN_ORDER) {
      Decision decision = nearest.resolve(metalake, tableObject, type);
      if (decision.kind() == Decision.Kind.CONFLICT) {
        LOG.error(
            "Skip commit-job enqueue for table {}: ambiguous {} policies {} on nearest level",
            tableId,
            type,
            decision.conflictingPolicyIds());
        return Optional.empty();
      }
      Optional<Long> winnerId = decision.winnerPolicyId();
      if (winnerId.isEmpty()) {
        continue;
      }
      Optional<PolicyEntity> loaded = policyLoader.apply(winnerId.get());
      if (loaded.isEmpty()) {
        continue;
      }
      PolicyEntity policy = loaded.get();
      if (!policy.enabled() || !Boolean.TRUE.equals(commitTrigger(policy.content()))) {
        continue;
      }
      if (MaintenanceJobTemplates.maintenanceType(policy.policyType()).isEmpty()) {
        continue;
      }
      chain.add(policy);
    }
    return Optional.of(new Chain(chain, tableRef));
  }

  @Nullable
  private static Long policyMinIntervalMs(@Nullable PolicyContent content) {
    if (!(content instanceof ExecutablePolicyContent)) {
      return null;
    }
    return ((ExecutablePolicyContent) content).executionSpec().minIntervalMs();
  }

  @Nullable
  private static Boolean commitTrigger(@Nullable PolicyContent content) {
    if (!(content instanceof ExecutablePolicyContent)) {
      return null;
    }
    return ((ExecutablePolicyContent) content).executionSpec().commitTrigger();
  }

  private static boolean usableTableIdentifier(@Nullable NameIdentifier identifier) {
    if (identifier == null || identifier.name() == null || identifier.name().isBlank()) {
      return false;
    }
    Namespace namespace = identifier.namespace();
    if (namespace == null || namespace.length() < 3) {
      return false;
    }
    for (int i = 0; i < namespace.length(); i++) {
      String level = namespace.level(i);
      if (level == null || level.isBlank()) {
        return false;
      }
    }
    return true;
  }

  private static String sparkTableIdentifier(NameIdentifier identifier) {
    Namespace namespace = identifier.namespace();
    StringBuilder builder = new StringBuilder();
    for (int i = 2; i < namespace.length(); i++) {
      if (builder.length() > 0) {
        builder.append('.');
      }
      builder.append(namespace.level(i));
    }
    if (builder.length() > 0) {
      builder.append('.');
    }
    builder.append(identifier.name());
    return builder.toString();
  }

  /** OnCommit policies plus the resolved Spark table ref. */
  static final class Chain {
    private final List<PolicyEntity> policies;
    private final ResolvedTableRef tableRef;

    Chain(List<PolicyEntity> policies, ResolvedTableRef tableRef) {
      this.policies = List.copyOf(Preconditions.checkNotNull(policies, "policies"));
      this.tableRef = Preconditions.checkNotNull(tableRef, "tableRef");
    }
  }
}
