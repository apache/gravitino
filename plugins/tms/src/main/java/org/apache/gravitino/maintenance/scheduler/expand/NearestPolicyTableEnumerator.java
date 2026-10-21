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
package org.apache.gravitino.maintenance.scheduler.expand;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.maintenance.scheduler.ExpandOccupancyGate;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.storage.relational.service.PolicyTagRelService;
import org.apache.gravitino.storage.relational.service.SchemaMetaService;
import org.apache.gravitino.storage.relational.service.TableMetaService;
import org.apache.gravitino.storage.relational.service.TagMetaService;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lists tables for which {@code policy} is the nearest maintenance policy of its type.
 *
 * <p>Walks {@code policy → tags → metadata objects}, expands catalog/schema attachments to tables,
 * then keeps a table only when {@link NearestMaintenancePolicyResolver} selects this policy id and
 * {@link ExpandOccupancyGate} allows enqueue. Scanning continues until {@code limit} enqueueable
 * tables are found (cooldown / in-flight winners do not consume the batch budget).
 */
public final class NearestPolicyTableEnumerator implements PolicyTableEnumerator {

  private static final Logger LOG = LoggerFactory.getLogger(NearestPolicyTableEnumerator.class);

  private final PolicyTagRelService policyTagRelService;
  private final TagMetaService tagMetaService;
  private final SchemaMetaService schemaMetaService;
  private final TableMetaService tableMetaService;
  private final Supplier<NearestMaintenancePolicyResolver> nearestResolverSupplier;

  /** Uses relational singletons and {@link GravitinoEnv#entityStore()}. */
  public NearestPolicyTableEnumerator() {
    this(
        PolicyTagRelService.getInstance(),
        TagMetaService.getInstance(),
        SchemaMetaService.getInstance(),
        TableMetaService.getInstance(),
        () -> new NearestMaintenancePolicyResolver(GravitinoEnv.getInstance().entityStore()));
  }

  /**
   * @param policyTagRelService policy-tag relations
   * @param tagMetaService tag metadata
   * @param schemaMetaService schema listing
   * @param tableMetaService table listing
   * @param nearestResolverSupplier nearest maintenance-policy resolver
   */
  public NearestPolicyTableEnumerator(
      PolicyTagRelService policyTagRelService,
      TagMetaService tagMetaService,
      SchemaMetaService schemaMetaService,
      TableMetaService tableMetaService,
      Supplier<NearestMaintenancePolicyResolver> nearestResolverSupplier) {
    this.policyTagRelService = policyTagRelService;
    this.tagMetaService = tagMetaService;
    this.schemaMetaService = schemaMetaService;
    this.tableMetaService = tableMetaService;
    this.nearestResolverSupplier = nearestResolverSupplier;
  }

  @Override
  public List<ExpandTableCandidate> listTableIds(
      PolicyEntity policy, int limit, ExpandOccupancyGate occupancyGate, long minIntervalMs) {
    if (limit <= 0) {
      return List.of();
    }
    Policy.BuiltInType policyType = policy.policyType();
    String metalake = policy.namespace().level(0);
    NameIdentifier policyIdent = NameIdentifierUtil.ofPolicy(metalake, policy.name());
    List<RelationalEntity<?>> tagEdges =
        policyTagRelService.listRelations(List.of(policyIdent), Entity.EntityType.POLICY);

    NearestMaintenancePolicyResolver nearest = nearestResolverSupplier.get();
    Set<Long> seenTables = new LinkedHashSet<>();
    Map<Long, ExpandTableCandidate> enqueueable = new LinkedHashMap<>();
    long nowMs = System.currentTimeMillis();
    ScanContext ctx =
        new ScanContext(
            metalake,
            policy.id(),
            policyType,
            nearest,
            occupancyGate,
            minIntervalMs,
            nowMs,
            seenTables,
            enqueueable,
            limit);

    for (RelationalEntity<?> edge : tagEdges) {
      Object target = edge.targetEntity();
      if (!(target instanceof TagEntity)) {
        continue;
      }
      TagEntity tag = (TagEntity) target;
      List<GenericEntity> objects;
      try {
        objects = tagMetaService.listAssociatedMetadataObjectsForTag(tag.nameIdentifier());
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to list objects for tag " + tag.nameIdentifier(), e);
      }
      for (GenericEntity object : objects) {
        if (enqueueable.size() >= limit) {
          return new ArrayList<>(enqueueable.values());
        }
        collectCandidates(object, ctx);
      }
    }
    return new ArrayList<>(enqueueable.values());
  }

  private void collectCandidates(GenericEntity object, ScanContext ctx) {
    MetadataObject.Type objectType;
    try {
      objectType = MetadataObject.Type.valueOf(object.type().name());
    } catch (IllegalArgumentException e) {
      return;
    }
    if (object.name() == null || object.name().isBlank()) {
      return;
    }
    MetadataObject metadataObject;
    try {
      metadataObject = MetadataObjects.parse(object.name(), objectType);
    } catch (RuntimeException e) {
      return;
    }

    switch (objectType) {
      case TABLE:
        if (object.id() == null) {
          return;
        }
        considerTable(object.id(), metadataObject, ctx);
        break;
      case SCHEMA:
        expandSchema(metadataObject, ctx);
        break;
      case CATALOG:
        expandCatalog(metadataObject, ctx);
        break;
      default:
        break;
    }
  }

  private void expandCatalog(MetadataObject catalog, ScanContext ctx) {
    List<SchemaEntity> schemas =
        schemaMetaService.listSchemasByNamespace(
            NamespaceUtil.ofSchema(ctx.metalake, catalog.name()));
    for (SchemaEntity schema : schemas) {
      if (ctx.enqueueable.size() >= ctx.limit) {
        return;
      }
      MetadataObject schemaObject =
          MetadataObjects.of(catalog.name(), schema.name(), MetadataObject.Type.SCHEMA);
      expandSchema(schemaObject, ctx);
    }
  }

  private void expandSchema(MetadataObject schema, ScanContext ctx) {
    String catalogName = schema.parent();
    if (catalogName == null) {
      return;
    }
    List<TableEntity> tables =
        tableMetaService.listTablesByNamespace(
            NamespaceUtil.ofTable(ctx.metalake, catalogName, schema.name()));
    for (TableEntity table : tables) {
      if (ctx.enqueueable.size() >= ctx.limit) {
        return;
      }
      MetadataObject tableObject =
          NameIdentifierUtil.toMetadataObject(table.nameIdentifier(), Entity.EntityType.TABLE);
      considerTable(table.id(), tableObject, ctx);
    }
  }

  private static void considerTable(long tableId, MetadataObject tableObject, ScanContext ctx) {
    if (tableId <= 0 || !ctx.seenTables.add(tableId)) {
      return;
    }
    NearestMaintenancePolicyResolver.Decision decision =
        ctx.nearest.resolve(ctx.metalake, tableObject, ctx.policyType);
    if (decision.kind() == NearestMaintenancePolicyResolver.Decision.Kind.CONFLICT) {
      LOG.error(
          "Skip table {} for maintenance type {}: ambiguous policies {} on nearest level {} ({})",
          tableObject.fullName(),
          ctx.policyType,
          decision.conflictingPolicyIds(),
          decision.conflictLevel() == null ? "?" : decision.conflictLevel().type(),
          decision.conflictLevel() == null ? "?" : decision.conflictLevel().fullName());
      return;
    }
    if (decision.winnerPolicyId().orElse(-1L) != ctx.policyId) {
      return;
    }
    MetadataObject winnerLevel = decision.winnerLevel();
    if (winnerLevel == null) {
      return;
    }
    if (ctx.occupancyGate.shouldSkip(tableId, ctx.policyId, ctx.minIntervalMs, ctx.nowMs)) {
      LOG.debug(
          "Skip cron-job enqueue for table {} policy {} (in-flight or within minIntervalMs={})",
          tableId,
          ctx.policyId,
          ctx.minIntervalMs);
      return;
    }
    ctx.enqueueable.put(tableId, new ExpandTableCandidate(tableId, winnerLevel.type()));
  }

  private static final class ScanContext {
    private final String metalake;
    private final long policyId;
    private final Policy.BuiltInType policyType;
    private final NearestMaintenancePolicyResolver nearest;
    private final ExpandOccupancyGate occupancyGate;
    private final long minIntervalMs;
    private final long nowMs;
    private final Set<Long> seenTables;
    private final Map<Long, ExpandTableCandidate> enqueueable;
    private final int limit;

    private ScanContext(
        String metalake,
        long policyId,
        Policy.BuiltInType policyType,
        NearestMaintenancePolicyResolver nearest,
        ExpandOccupancyGate occupancyGate,
        long minIntervalMs,
        long nowMs,
        Set<Long> seenTables,
        Map<Long, ExpandTableCandidate> enqueueable,
        int limit) {
      this.metalake = metalake;
      this.policyId = policyId;
      this.policyType = policyType;
      this.nearest = nearest;
      this.occupancyGate = occupancyGate;
      this.minIntervalMs = minIntervalMs;
      this.nowMs = nowMs;
      this.seenTables = seenTables;
      this.enqueueable = enqueueable;
      this.limit = limit;
    }
  }
}
