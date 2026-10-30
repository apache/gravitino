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
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.maintenance.scheduler.ResolvedTableRef;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceJobTemplates;
import org.apache.gravitino.maintenance.scheduler.policy.expand.NearestMaintenancePolicyResolver;
import org.apache.gravitino.maintenance.scheduler.policy.expand.NearestMaintenancePolicyResolver.Decision;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.ExecutablePolicyContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.storage.relational.service.PolicyMetaService;
import org.apache.gravitino.utils.NameIdentifierUtil;
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

  private final Function<Long, Optional<PolicyEntity>> policyLoader;
  private final Supplier<EntityStore> entityStoreSupplier;

  /**
   * Production collector using {@link PolicyMetaService} and {@link GravitinoEnv#entityStore()}.
   */
  public CommitChainCollector() {
    this(
        id -> Optional.ofNullable(PolicyMetaService.getInstance().getPolicyById(id)),
        () -> GravitinoEnv.getInstance().entityStore());
  }

  /**
   * Visible for tests.
   *
   * @param policyLoader loads policies by id
   * @param entityStoreSupplier entity store supplier
   */
  CommitChainCollector(
      Function<Long, Optional<PolicyEntity>> policyLoader,
      Supplier<EntityStore> entityStoreSupplier) {
    this.policyLoader = Preconditions.checkNotNull(policyLoader);
    this.entityStoreSupplier = Preconditions.checkNotNull(entityStoreSupplier);
  }

  /**
   * Collects the onCommit chain for a table whose metalake, catalog, and schema.table are already
   * known from the commit event.
   *
   * @param tableId table entity id
   * @param identifier {@code metalake.catalog.schema.table} from the commit event
   * @return collected chain (members may be empty) when the identifier names a table; empty when it
   *     does not
   */
  public Optional<CollectedCommitChain> collect(long tableId, NameIdentifier identifier) {
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
        new NearestMaintenancePolicyResolver(entityStoreSupplier.get());
    List<PolicyEntity> chain = new ArrayList<>();
    for (Policy.BuiltInType type : CHAIN_ORDER) {
      Decision decision = nearest.resolve(metalake, tableObject, type);
      if (decision.kind() == Decision.Kind.CONFLICT) {
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
    return Optional.of(new CollectedCommitChain(chain, tableRef));
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

  /**
   * {@code schema.table}, including extra schema levels when the namespace is deeper than three.
   */
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
}
