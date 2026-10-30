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
 * Finds tables where {@code policy} is the nearest maintenance policy of its type.
 *
 * <p>Flow: {@code policy → tags → tagged objects}. Catalog/schema tags expand to tables (including
 * hierarchical child schemas). A table is kept only when {@link NearestMaintenancePolicyResolver}
 * picks this policy and {@link ExpandOccupancyGate} allows enqueue. Stops after {@code limit}
 * accepted tables (skipped cooldown / in-flight tables do not count).
 *
 * <p>Catalog/schema expansion prefers live {@code listSchemas}/{@code listTables}, then imports
 * only missing entities with {@code loadSchema}/{@code loadTable}. On list failure, falls back to
 * the relational store. A direct table tag uses the stored table id as-is.
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

  /**
   * Production wiring via relational singletons and {@link GravitinoEnv}.
   *
   * <p>Resolves {@link GravitinoEnv#entityStore()} lazily so scheduler pool startup can construct
   * this enumerator before the env is fully initialized (unit tests). Dispatchers may be null until
   * env init; expand then falls back to relational meta listing.
   */
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
   * @param policyTagRelService policy ↔ tag edges
   * @param tagMetaService tagged metadata objects
   * @param schemaMetaService schemas already in the entity store
   * @param tableMetaService tables already in the entity store
   * @param nearestResolverSupplier decides which policy wins for a table
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
   * @param policyTagRelService policy ↔ tag edges
   * @param tagMetaService tagged metadata objects
   * @param schemaMetaService schemas already in the entity store
   * @param tableMetaService tables already in the entity store
   * @param nearestResolverSupplier decides which policy wins for a table
   * @param schemaDispatcher optional live catalog schema list/import
   * @param tableDispatcher optional live catalog table list/import
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

    String metalake = policy.namespace().level(0);
    NameIdentifier policyIdent = NameIdentifierUtil.ofPolicy(metalake, policy.name());
    List<RelationalEntity<?>> policyTagEdges =
        policyTagRelService.listRelations(List.of(policyIdent), Entity.EntityType.POLICY);

    ScanState scan =
        new ScanState(
            metalake,
            policy.id(),
            policy.policyType(),
            nearestResolverSupplier.get(),
            occupancyGate,
            minIntervalMs,
            System.currentTimeMillis(),
            new LinkedHashSet<>(),
            new LinkedHashMap<>(),
            limit);

    for (RelationalEntity<?> edge : policyTagEdges) {
      Object target = edge.targetEntity();
      if (!(target instanceof TagEntity)) {
        continue;
      }
      TagEntity tag = (TagEntity) target;
      List<GenericEntity> taggedObjects;
      try {
        taggedObjects = tagMetaService.listAssociatedMetadataObjectsForTag(tag.nameIdentifier());
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to list objects for tag " + tag.nameIdentifier(), e);
      }
      for (GenericEntity taggedObject : taggedObjects) {
        if (scan.batchFull()) {
          return new ArrayList<>(scan.acceptedByTableId.values());
        }
        expandTaggedObject(taggedObject, scan);
      }
    }
    return new ArrayList<>(scan.acceptedByTableId.values());
  }

  /** Expands one tagged catalog/schema/table into candidate tables. */
  private void expandTaggedObject(GenericEntity taggedObject, ScanState scan) {
    MetadataObject.Type objectType;
    try {
      objectType = MetadataObject.Type.valueOf(taggedObject.type().name());
    } catch (IllegalArgumentException e) {
      return;
    }
    if (taggedObject.name() == null || taggedObject.name().isBlank()) {
      return;
    }

    MetadataObject metadataObject;
    try {
      metadataObject = MetadataObjects.parse(taggedObject.name(), objectType);
    } catch (RuntimeException e) {
      return;
    }

    switch (objectType) {
      case TABLE:
        if (taggedObject.id() != null) {
          tryEnqueueTable(taggedObject.id(), metadataObject, scan);
        }
        break;
      case SCHEMA:
        // Parent schema tags also cover hierarchical children (e.g. a → a:b).
        expandSchema(metadataObject, scan, /*includeChildSchemas=*/ true);
        break;
      case CATALOG:
        expandCatalog(metadataObject, scan);
        break;
      default:
        break;
    }
  }

  /** Lists schemas under a catalog tag, imports missing ones, then expands each to tables. */
  private void expandCatalog(MetadataObject catalog, ScanState scan) {
    if (schemaDispatcher != null) {
      try {
        NameIdentifier[] liveSchemas =
            schemaDispatcher.listSchemas(NamespaceUtil.ofSchema(scan.metalake, catalog.name()));
        Set<String> storedSchemaNames = storedSchemaNames(scan.metalake, catalog.name());
        for (NameIdentifier schemaIdent : liveSchemas) {
          if (scan.batchFull()) {
            return;
          }
          if (!importSchemaIfMissing(schemaIdent, storedSchemaNames)) {
            continue;
          }
          expandSchema(
              MetadataObjects.of(catalog.name(), schemaIdent.name(), MetadataObject.Type.SCHEMA),
              scan,
              false);
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

    for (SchemaEntity schema :
        schemaMetaService.listSchemasByNamespace(
            NamespaceUtil.ofSchema(scan.metalake, catalog.name()))) {
      if (scan.batchFull()) {
        return;
      }
      expandSchema(
          MetadataObjects.of(catalog.name(), schema.name(), MetadataObject.Type.SCHEMA),
          scan,
          false);
    }
  }

  /**
   * Expands tables in {@code schema}, and optionally hierarchical child schemas under it.
   *
   * @param includeChildSchemas when true (schema tag), also walk descendants such as {@code a:b}
   */
  private void expandSchema(MetadataObject schema, ScanState scan, boolean includeChildSchemas) {
    String catalogName = schema.parent();
    if (catalogName == null || schema.name() == null || schema.name().isBlank()) {
      return;
    }

    expandTablesInSchema(catalogName, schema.name(), scan);
    if (!includeChildSchemas || scan.batchFull()) {
      return;
    }

    String childPrefix = schema.name() + HierarchicalSchemaUtil.schemaSeparator();
    if (schemaDispatcher != null) {
      try {
        NameIdentifier[] liveSchemas =
            schemaDispatcher.listSchemas(NamespaceUtil.ofSchema(scan.metalake, catalogName));
        Set<String> storedSchemaNames = storedSchemaNames(scan.metalake, catalogName);
        for (NameIdentifier childSchema : liveSchemas) {
          if (scan.batchFull()) {
            return;
          }
          if (childSchema.name() == null || !childSchema.name().startsWith(childPrefix)) {
            continue;
          }
          if (!importSchemaIfMissing(childSchema, storedSchemaNames)) {
            continue;
          }
          expandTablesInSchema(catalogName, childSchema.name(), scan);
        }
        return;
      } catch (RuntimeException e) {
        LOG.warn(
            "Failed to list child schemas under {} from catalog; falling back to meta: {}",
            schema.fullName(),
            e.toString());
      }
    }

    for (SchemaEntity child :
        schemaMetaService.listSchemasByNamespace(
            NamespaceUtil.ofSchema(scan.metalake, catalogName))) {
      if (scan.batchFull()) {
        return;
      }
      if (child.name() == null || !child.name().startsWith(childPrefix)) {
        continue;
      }
      expandTablesInSchema(catalogName, child.name(), scan);
    }
  }

  /** Lists tables in one schema: live catalog first, entity store on failure. */
  private void expandTablesInSchema(String catalogName, String schemaName, ScanState scan) {
    Namespace tableNamespace = NamespaceUtil.ofTable(scan.metalake, catalogName, schemaName);
    if (tableDispatcher != null) {
      try {
        NameIdentifier[] liveTables = tableDispatcher.listTables(tableNamespace);
        Map<String, TableEntity> storedTables = storedTablesByName(tableNamespace);
        for (NameIdentifier tableIdent : liveTables) {
          if (scan.batchFull()) {
            return;
          }
          if (!importTableIfMissing(tableIdent, storedTables)) {
            continue;
          }
          tryEnqueueTable(
              storedTables.get(tableIdent.name()).id(),
              NameIdentifierUtil.toMetadataObject(tableIdent, Entity.EntityType.TABLE),
              scan);
        }
        return;
      } catch (NoSuchSchemaException e) {
        LOG.warn(
            "Schema {}.{}.{} missing for policy expand; falling back to meta: {}",
            scan.metalake,
            catalogName,
            schemaName,
            e.toString());
      } catch (RuntimeException e) {
        LOG.warn(
            "Failed to list tables from catalog for {}.{}.{}; falling back to meta: {}",
            scan.metalake,
            catalogName,
            schemaName,
            e.toString());
      }
    }

    for (TableEntity table : tableMetaService.listTablesByNamespace(tableNamespace)) {
      if (scan.batchFull()) {
        return;
      }
      tryEnqueueTable(
          table.id(),
          NameIdentifierUtil.toMetadataObject(table.nameIdentifier(), Entity.EntityType.TABLE),
          scan);
    }
  }

  /**
   * Ensures {@code schemaIdent} exists in the entity store.
   *
   * @return {@code true} if already stored or import succeeded; {@code false} if import failed
   */
  private boolean importSchemaIfMissing(NameIdentifier schemaIdent, Set<String> storedSchemaNames) {
    if (storedSchemaNames.contains(schemaIdent.name())) {
      return true;
    }
    if (schemaDispatcher == null) {
      return false;
    }
    try {
      schemaDispatcher.loadSchema(schemaIdent);
      storedSchemaNames.add(schemaIdent.name());
      return true;
    } catch (RuntimeException e) {
      LOG.warn(
          "Skip schema {} while importing missing entity for policy expand: {}",
          schemaIdent,
          e.toString());
      return false;
    }
  }

  /**
   * Ensures {@code tableIdent} exists in the entity store and records it in {@code storedTables}.
   *
   * @return {@code true} if already stored or import succeeded; {@code false} if import failed
   */
  private boolean importTableIfMissing(
      NameIdentifier tableIdent, Map<String, TableEntity> storedTables) {
    if (storedTables.containsKey(tableIdent.name())) {
      return true;
    }
    if (tableDispatcher == null) {
      return false;
    }
    try {
      Table loaded = tableDispatcher.loadTable(tableIdent);
      TableEntity entity = resolveStoredTable(loaded, tableIdent);
      if (entity == null || entity.id() == null) {
        LOG.warn("Skip table {} after import load: missing entity id", tableIdent);
        return false;
      }
      storedTables.put(tableIdent.name(), entity);
      return true;
    } catch (NoSuchTableException e) {
      LOG.debug("Table {} disappeared during expand: {}", tableIdent, e.toString());
      return false;
    } catch (RuntimeException e) {
      LOG.warn("Skip table {} during catalog expand import: {}", tableIdent, e.toString());
      return false;
    }
  }

  private Set<String> storedSchemaNames(String metalake, String catalogName) {
    Set<String> names = new HashSet<>();
    for (SchemaEntity schema :
        schemaMetaService.listSchemasByNamespace(NamespaceUtil.ofSchema(metalake, catalogName))) {
      if (schema.name() != null) {
        names.add(schema.name());
      }
    }
    return names;
  }

  private Map<String, TableEntity> storedTablesByName(Namespace tableNamespace) {
    Map<String, TableEntity> byName = new HashMap<>();
    for (TableEntity table : tableMetaService.listTablesByNamespace(tableNamespace)) {
      if (table.name() != null) {
        byName.put(table.name(), table);
      }
    }
    return byName;
  }

  /** Prefer the entity on {@link EntityCombinedTable}; otherwise look up the store. */
  @Nullable
  private TableEntity resolveStoredTable(Table table, NameIdentifier tableIdent) {
    if (table instanceof EntityCombinedTable) {
      TableEntity entity = ((EntityCombinedTable) table).tableFromGravitino();
      if (entity != null && entity.id() != null) {
        return entity;
      }
    }
    try {
      for (TableEntity entity : tableMetaService.listTablesByNamespace(tableIdent.namespace())) {
        if (tableIdent.name().equals(entity.name())) {
          return entity;
        }
      }
    } catch (RuntimeException e) {
      LOG.warn("Failed to resolve table entity for {}: {}", tableIdent, e.toString());
    }
    return null;
  }

  /**
   * Accepts {@code tableId} into the batch when this policy is the nearest winner and occupancy
   * allows enqueue.
   */
  private static void tryEnqueueTable(long tableId, MetadataObject tableObject, ScanState scan) {
    if (tableId <= 0 || !scan.visitedTableIds.add(tableId)) {
      return;
    }

    NearestMaintenancePolicyResolver.Decision decision =
        scan.nearestResolver.resolve(scan.metalake, tableObject, scan.policyType);
    if (decision.kind() == NearestMaintenancePolicyResolver.Decision.Kind.CONFLICT) {
      LOG.error(
          "Skip table {} for maintenance type {}: ambiguous policies {} on nearest level {} ({})",
          tableObject.fullName(),
          scan.policyType,
          decision.conflictingPolicyIds(),
          decision.conflictLevel() == null ? "?" : decision.conflictLevel().type(),
          decision.conflictLevel() == null ? "?" : decision.conflictLevel().fullName());
      return;
    }
    if (decision.winnerPolicyId().orElse(-1L) != scan.policyId) {
      return;
    }
    MetadataObject winnerLevel = decision.winnerLevel();
    if (winnerLevel == null) {
      return;
    }
    if (scan.occupancyGate.shouldSkip(tableId, scan.policyId, scan.minIntervalMs, scan.nowMs)) {
      LOG.debug(
          "Skip cron-job enqueue for table {} policy {} (in-flight or within minIntervalMs={})",
          tableId,
          scan.policyId,
          scan.minIntervalMs);
      return;
    }
    scan.acceptedByTableId.put(tableId, new ExpandTableCandidate(tableId, winnerLevel.type()));
  }

  /** Mutable progress for one {@link #listTableIds} call. */
  private static final class ScanState {
    private final String metalake;
    private final long policyId;
    private final Policy.BuiltInType policyType;
    private final NearestMaintenancePolicyResolver nearestResolver;
    private final ExpandOccupancyGate occupancyGate;
    private final long minIntervalMs;
    private final long nowMs;
    private final Set<Long> visitedTableIds;
    private final Map<Long, ExpandTableCandidate> acceptedByTableId;
    private final int limit;

    private ScanState(
        String metalake,
        long policyId,
        Policy.BuiltInType policyType,
        NearestMaintenancePolicyResolver nearestResolver,
        ExpandOccupancyGate occupancyGate,
        long minIntervalMs,
        long nowMs,
        Set<Long> visitedTableIds,
        Map<Long, ExpandTableCandidate> acceptedByTableId,
        int limit) {
      this.metalake = metalake;
      this.policyId = policyId;
      this.policyType = policyType;
      this.nearestResolver = nearestResolver;
      this.occupancyGate = occupancyGate;
      this.minIntervalMs = minIntervalMs;
      this.nowMs = nowMs;
      this.visitedTableIds = visitedTableIds;
      this.acceptedByTableId = acceptedByTableId;
      this.limit = limit;
    }

    private boolean batchFull() {
      return acceptedByTableId.size() >= limit;
    }
  }
}
