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
package org.apache.gravitino.maintenance.scheduler.policy.expand;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.catalog.EntityCombinedTable;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.storage.relational.service.PolicyTagRelService;
import org.apache.gravitino.storage.relational.service.SchemaMetaService;
import org.apache.gravitino.storage.relational.service.TableMetaService;
import org.apache.gravitino.storage.relational.service.TagMetaService;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lists tables for which {@code policy} is the nearest maintenance policy of its type.
 *
 * <p>Walks {@code policy → tags → metadata objects}, expands catalog/schema attachments to tables
 * (including tables in hierarchical descendant schemas), then keeps a table only when {@link
 * NearestMaintenancePolicyResolver} selects this policy id and {@link ExpandOccupancyGate} allows
 * enqueue. Scanning continues until {@code limit} enqueueable tables are found (cooldown /
 * in-flight winners do not consume the batch budget).
 *
 * <p>For catalog/schema attachments, prefers {@link SchemaDispatcher}/{@link TableDispatcher}
 * {@code list*} against the live catalog, then imports only schemas/tables missing from the entity
 * store via {@code loadSchema}/{@code loadTable}. Already-stored entities are used by id without
 * reloading. Falls back to relational meta listing when dispatchers are unavailable or listing
 * fails. Direct table-tagged objects still use the stored table id.
 */
public final class NearestPolicyTableEnumerator implements PolicyTableEnumerator {

  private static final Logger LOG = LoggerFactory.getLogger(NearestPolicyTableEnumerator.class);

  private final PolicyTagRelService policyTagRelService;
  private final TagMetaService tagMetaService;
  private final SchemaMetaService schemaMetaService;
  private final TableMetaService tableMetaService;
  private final Supplier<NearestMaintenancePolicyResolver> nearestResolverSupplier;
  @Nullable private final SchemaDispatcher schemaDispatcher;
  @Nullable private final TableDispatcher tableDispatcher;

  /** Uses relational singletons and {@link GravitinoEnv} dispatchers / entity store. */
  public NearestPolicyTableEnumerator() {
    this(
        PolicyTagRelService.getInstance(),
        TagMetaService.getInstance(),
        SchemaMetaService.getInstance(),
        TableMetaService.getInstance(),
        () -> new NearestMaintenancePolicyResolver(GravitinoEnv.getInstance().entityStore()),
        GravitinoEnv.getInstance().internalSchemaDispatcher(),
        GravitinoEnv.getInstance().internalTableDispatcher());
  }

  /**
   * Meta-only constructor for unit tests (no catalog list/import).
   *
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
    this(
        policyTagRelService,
        tagMetaService,
        schemaMetaService,
        tableMetaService,
        nearestResolverSupplier,
        null,
        null);
  }

  /**
   * @param policyTagRelService policy-tag relations
   * @param tagMetaService tag metadata
   * @param schemaMetaService schema listing
   * @param tableMetaService table listing
   * @param nearestResolverSupplier nearest maintenance-policy resolver
   * @param schemaDispatcher optional schema dispatcher for catalog list/import
   * @param tableDispatcher optional table dispatcher for catalog list/import
   */
  public NearestPolicyTableEnumerator(
      PolicyTagRelService policyTagRelService,
      TagMetaService tagMetaService,
      SchemaMetaService schemaMetaService,
      TableMetaService tableMetaService,
      Supplier<NearestMaintenancePolicyResolver> nearestResolverSupplier,
      @Nullable SchemaDispatcher schemaDispatcher,
      @Nullable TableDispatcher tableDispatcher) {
    this.policyTagRelService = policyTagRelService;
    this.tagMetaService = tagMetaService;
    this.schemaMetaService = schemaMetaService;
    this.tableMetaService = tableMetaService;
    this.nearestResolverSupplier = nearestResolverSupplier;
    this.schemaDispatcher = schemaDispatcher;
    this.tableDispatcher = tableDispatcher;
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
        // A tag on a parent schema also covers tables in child schemas such as {@code a:b}.
        expandSchema(metadataObject, ctx, true);
        break;
      case CATALOG:
        expandCatalog(metadataObject, ctx);
        break;
      default:
        break;
    }
  }

  private void expandCatalog(MetadataObject catalog, ScanContext ctx) {
    if (schemaDispatcher != null) {
      try {
        Namespace catalogNs = NamespaceUtil.ofSchema(ctx.metalake, catalog.name());
        NameIdentifier[] schemas = schemaDispatcher.listSchemas(catalogNs);
        Set<String> knownSchemas = knownSchemaNames(ctx.metalake, catalog.name());
        for (NameIdentifier schemaIdent : schemas) {
          if (ctx.enqueueable.size() >= ctx.limit) {
            return;
          }
          if (!importSchemaIfMissing(schemaIdent, knownSchemas)) {
            continue;
          }
          MetadataObject schemaObject =
              MetadataObjects.of(catalog.name(), schemaIdent.name(), MetadataObject.Type.SCHEMA);
          expandSchema(schemaObject, ctx, false);
        }
        return;
      } catch (NoSuchCatalogException e) {
        LOG.warn(
            "Catalog {} missing while expanding policy attachment; falling back to meta list: {}",
            catalog.fullName(),
            e.toString());
      } catch (RuntimeException e) {
        LOG.warn(
            "Failed to list schemas from catalog {} for policy expand; falling back to meta: {}",
            catalog.fullName(),
            e.toString());
      }
    }

    List<SchemaEntity> schemas =
        schemaMetaService.listSchemasByNamespace(
            NamespaceUtil.ofSchema(ctx.metalake, catalog.name()));
    for (SchemaEntity schema : schemas) {
      if (ctx.enqueueable.size() >= ctx.limit) {
        return;
      }
      MetadataObject schemaObject =
          MetadataObjects.of(catalog.name(), schema.name(), MetadataObject.Type.SCHEMA);
      // Catalog listing already includes every hierarchical schema name.
      expandSchema(schemaObject, ctx, false);
    }
  }

  private void expandSchema(MetadataObject schema, ScanContext ctx, boolean includeDescendants) {
    String catalogName = schema.parent();
    if (catalogName == null || schema.name() == null || schema.name().isBlank()) {
      return;
    }
    expandTables(catalogName, schema.name(), ctx);
    if (!includeDescendants || ctx.enqueueable.size() >= ctx.limit) {
      return;
    }
    String separator = HierarchicalSchemaUtil.schemaSeparator();
    String descendantPrefix = schema.name() + separator;

    if (schemaDispatcher != null) {
      try {
        NameIdentifier[] schemas =
            schemaDispatcher.listSchemas(NamespaceUtil.ofSchema(ctx.metalake, catalogName));
        Set<String> knownSchemas = knownSchemaNames(ctx.metalake, catalogName);
        for (NameIdentifier descendant : schemas) {
          if (ctx.enqueueable.size() >= ctx.limit) {
            return;
          }
          if (descendant.name() == null || !descendant.name().startsWith(descendantPrefix)) {
            continue;
          }
          if (!importSchemaIfMissing(descendant, knownSchemas)) {
            continue;
          }
          expandTables(catalogName, descendant.name(), ctx);
        }
        return;
      } catch (RuntimeException e) {
        LOG.warn(
            "Failed to list descendant schemas under {} from catalog; falling back to meta: {}",
            schema.fullName(),
            e.toString());
      }
    }

    List<SchemaEntity> schemas =
        schemaMetaService.listSchemasByNamespace(NamespaceUtil.ofSchema(ctx.metalake, catalogName));
    for (SchemaEntity descendant : schemas) {
      if (ctx.enqueueable.size() >= ctx.limit) {
        return;
      }
      if (descendant.name() == null || !descendant.name().startsWith(descendantPrefix)) {
        continue;
      }
      expandTables(catalogName, descendant.name(), ctx);
    }
  }

  private void expandTables(String catalogName, String schemaName, ScanContext ctx) {
    if (tableDispatcher != null && schemaDispatcher != null) {
      try {
        Namespace tableNs = NamespaceUtil.ofTable(ctx.metalake, catalogName, schemaName);
        NameIdentifier[] listed = tableDispatcher.listTables(tableNs);
        Map<String, TableEntity> knownTables = knownTablesByName(tableNs);
        for (NameIdentifier tableIdent : listed) {
          if (ctx.enqueueable.size() >= ctx.limit) {
            return;
          }
          TableEntity existing = knownTables.get(tableIdent.name());
          if (existing != null) {
            MetadataObject tableObject =
                NameIdentifierUtil.toMetadataObject(tableIdent, Entity.EntityType.TABLE);
            considerTable(existing.id(), tableObject, ctx);
            continue;
          }
          try {
            Table table = tableDispatcher.loadTable(tableIdent);
            Long tableId = resolveTableId(table, tableIdent);
            if (tableId == null) {
              LOG.warn("Skip table {} after import load: missing entity id", tableIdent);
              continue;
            }
            MetadataObject tableObject =
                NameIdentifierUtil.toMetadataObject(tableIdent, Entity.EntityType.TABLE);
            considerTable(tableId, tableObject, ctx);
          } catch (NoSuchTableException e) {
            LOG.debug("Table {} disappeared during expand: {}", tableIdent, e.toString());
          } catch (RuntimeException e) {
            LOG.warn("Skip table {} during catalog expand import: {}", tableIdent, e.toString());
          }
        }
        return;
      } catch (NoSuchSchemaException e) {
        LOG.warn(
            "Schema {}.{}.{} missing for policy expand; falling back to meta: {}",
            ctx.metalake,
            catalogName,
            schemaName,
            e.toString());
      } catch (RuntimeException e) {
        LOG.warn(
            "Failed to list tables from catalog for {}.{}.{}; falling back to meta: {}",
            ctx.metalake,
            catalogName,
            schemaName,
            e.toString());
      }
    }

    List<TableEntity> tables =
        tableMetaService.listTablesByNamespace(
            NamespaceUtil.ofTable(ctx.metalake, catalogName, schemaName));
    for (TableEntity table : tables) {
      if (ctx.enqueueable.size() >= ctx.limit) {
        return;
      }
      MetadataObject tableObject =
          NameIdentifierUtil.toMetadataObject(table.nameIdentifier(), Entity.EntityType.TABLE);
      considerTable(table.id(), tableObject, ctx);
    }
  }

  private Set<String> knownSchemaNames(String metalake, String catalogName) {
    List<SchemaEntity> stored =
        schemaMetaService.listSchemasByNamespace(NamespaceUtil.ofSchema(metalake, catalogName));
    Set<String> names = new HashSet<>();
    for (SchemaEntity schema : stored) {
      if (schema.name() != null) {
        names.add(schema.name());
      }
    }
    return names;
  }

  private Map<String, TableEntity> knownTablesByName(Namespace tableNs) {
    List<TableEntity> stored = tableMetaService.listTablesByNamespace(tableNs);
    Map<String, TableEntity> byName = new HashMap<>();
    for (TableEntity table : stored) {
      if (table.name() != null) {
        byName.put(table.name(), table);
      }
    }
    return byName;
  }

  /**
   * Imports {@code schemaIdent} when it is absent from the entity store.
   *
   * @return {@code false} when import was attempted and failed; {@code true} when already known or
   *     import succeeded
   */
  private boolean importSchemaIfMissing(NameIdentifier schemaIdent, Set<String> knownSchemas) {
    if (knownSchemas.contains(schemaIdent.name())) {
      return true;
    }
    if (schemaDispatcher == null) {
      return false;
    }
    try {
      schemaDispatcher.loadSchema(schemaIdent);
      knownSchemas.add(schemaIdent.name());
      return true;
    } catch (RuntimeException e) {
      LOG.warn(
          "Skip schema {} while importing missing entity for policy expand: {}",
          schemaIdent,
          e.toString());
      return false;
    }
  }

  @Nullable
  private Long resolveTableId(Table table, NameIdentifier tableIdent) {
    if (table instanceof EntityCombinedTable) {
      TableEntity entity = ((EntityCombinedTable) table).tableFromGravitino();
      if (entity != null && entity.id() != null) {
        return entity.id();
      }
    }
    try {
      List<TableEntity> stored = tableMetaService.listTablesByNamespace(tableIdent.namespace());
      for (TableEntity entity : stored) {
        if (tableIdent.name().equals(entity.name())) {
          return entity.id();
        }
      }
    } catch (RuntimeException e) {
      LOG.warn("Failed to resolve table id for {}: {}", tableIdent, e.toString());
    }
    return null;
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
