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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.maintenance.scheduler.PolicyLookup;
import org.apache.gravitino.maintenance.scheduler.cron.MaintenanceJobTemplates;
import org.apache.gravitino.maintenance.scheduler.cron.MaintenanceTableRefs;
import org.apache.gravitino.maintenance.scheduler.cron.MaintenanceTableRefs.ResolvedTableRef;
import org.apache.gravitino.maintenance.scheduler.expand.NearestMaintenancePolicyResolver;
import org.apache.gravitino.maintenance.scheduler.expand.NearestPolicyDecision;
import org.apache.gravitino.maintenance.storage.MetalakeNames;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.ExecutablePolicyContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the onCommit maintenance chain for a table: compaction → manifest-rewrite →
 * snapshot-expiry (§5.2.3). Orphan cleanup is timeTrigger-only and never included.
 */
public final class CommitChainCollector {

  private static final Logger LOG = LoggerFactory.getLogger(CommitChainCollector.class);

  /** Fixed commit-chain order. */
  public static final List<Policy.BuiltInType> CHAIN_ORDER =
      List.of(
          Policy.BuiltInType.ICEBERG_COMPACTION,
          Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS,
          Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION);

  private final PolicyLookup policyLookup;
  private final Supplier<EntityStore> entityStoreSupplier;
  private final Function<Long, Optional<TableContext>> tableContextResolver;

  /**
   * Production collector using {@link GravitinoEnv#entityStore()} and relational table refs.
   *
   * @param policyLookup loads policies by id
   */
  public CommitChainCollector(PolicyLookup policyLookup) {
    this(
        policyLookup,
        () -> GravitinoEnv.getInstance().entityStore(),
        CommitChainCollector::resolveTableContext);
  }

  /**
   * Visible for tests.
   *
   * @param policyLookup policy loader
   * @param entityStoreSupplier entity store supplier
   * @param tableContextResolver resolves metalake + metadata object for a table id
   */
  CommitChainCollector(
      PolicyLookup policyLookup,
      Supplier<EntityStore> entityStoreSupplier,
      Function<Long, Optional<TableContext>> tableContextResolver) {
    this.policyLookup = Preconditions.checkNotNull(policyLookup);
    this.entityStoreSupplier = Preconditions.checkNotNull(entityStoreSupplier);
    this.tableContextResolver = Preconditions.checkNotNull(tableContextResolver);
  }

  /**
   * Collects the onCommit chain and the Spark table ref in one metadata resolution pass.
   *
   * @param tableId table entity id
   * @return collected chain (members may be empty) when the table resolves; empty when metadata is
   *     unknown
   */
  public Optional<CollectedCommitChain> collect(long tableId) {
    Optional<TableContext> ctx = tableContextResolver.apply(tableId);
    if (ctx.isEmpty()) {
      LOG.info("Skipping commit chain for table {}: could not resolve table metadata", tableId);
      return Optional.empty();
    }
    TableContext table = ctx.get();
    NearestMaintenancePolicyResolver nearest =
        new NearestMaintenancePolicyResolver(entityStoreSupplier.get());
    List<CommitChainMember> chain = new ArrayList<>();
    for (Policy.BuiltInType type : CHAIN_ORDER) {
      NearestPolicyDecision decision = nearest.resolve(table.metalake(), table.tableObject(), type);
      if (decision.kind() == NearestPolicyDecision.Kind.CONFLICT) {
        // Match expand fail-closed: ambiguous same-level policies skip the whole table.
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
      Optional<PolicyEntity> loaded = policyLookup.findById(winnerId.get());
      if (loaded.isEmpty()) {
        continue;
      }
      PolicyEntity policy = loaded.get();
      if (!policy.enabled() || !Boolean.TRUE.equals(commitTrigger(policy.content()))) {
        continue;
      }
      Optional<String> wire = MaintenanceJobTemplates.maintenanceType(type);
      if (wire.isEmpty()) {
        continue;
      }
      chain.add(new CommitChainMember(type, policy, wire.get()));
    }
    return Optional.of(new CollectedCommitChain(chain, table.tableRef()));
  }

  @Nullable
  private static Boolean commitTrigger(@Nullable PolicyContent content) {
    if (!(content instanceof ExecutablePolicyContent)) {
      return null;
    }
    return ((ExecutablePolicyContent) content).executionSpec().commitTrigger();
  }

  private static Optional<TableContext> resolveTableContext(long tableId) {
    String metalake = MetalakeNames.forTableId(tableId);
    if (metalake == null || metalake.isEmpty()) {
      return Optional.empty();
    }
    List<ResolvedTableRef> refs = MaintenanceTableRefs.resolve(List.of(tableId));
    if (refs.isEmpty()) {
      return Optional.empty();
    }
    ResolvedTableRef ref = refs.get(0);
    String[] parts = ref.tableIdentifier().split("\\.");
    if (parts.length < 2) {
      return Optional.empty();
    }
    List<String> names = new ArrayList<>();
    names.add(ref.catalogName());
    names.addAll(Arrays.asList(parts));
    MetadataObject tableObject = MetadataObjects.of(names, MetadataObject.Type.TABLE);
    return Optional.of(new TableContext(metalake, tableObject, ref));
  }

  /** Metalake + metadata object + Spark ref for nearest-policy walks and snapshot build. */
  static final class TableContext {
    private final String metalake;
    private final MetadataObject tableObject;
    private final ResolvedTableRef tableRef;

    TableContext(String metalake, MetadataObject tableObject, ResolvedTableRef tableRef) {
      this.metalake = metalake;
      this.tableObject = tableObject;
      this.tableRef = tableRef;
    }

    String metalake() {
      return metalake;
    }

    MetadataObject tableObject() {
      return tableObject;
    }

    ResolvedTableRef tableRef() {
      return tableRef;
    }
  }
}
