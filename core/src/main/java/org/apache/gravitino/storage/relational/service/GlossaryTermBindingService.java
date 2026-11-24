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

package org.apache.gravitino.storage.relational.service;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermBindingMapper;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermMetaMapper;
import org.apache.gravitino.storage.relational.po.GlossaryTermBindingPO;
import org.apache.gravitino.storage.relational.po.GlossaryTermPO;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persistence and atomic cleanup for glossary term bindings.
 *
 * <p>All write methods join the caller's transaction. Cleanup methods expect the caller to hold the
 * deleting or altering endpoint lock first, then lock affected surviving term rows in ascending ID
 * order, retire the bindings, increment each affected surviving term's revision once, and publish
 * structured audit records only after the transaction commits.
 */
public final class GlossaryTermBindingService {
  private static final Logger LOG = LoggerFactory.getLogger(GlossaryTermBindingService.class);
  private static final int BATCH_SIZE = 500;
  private static final GlossaryTermBindingService INSTANCE = new GlossaryTermBindingService();

  /** Storage target types for glossary term bindings. */
  public enum TargetType {
    /** A catalog implementing a term. */
    CATALOG,
    /** A schema implementing a term. */
    SCHEMA,
    /** A table implementing a term. */
    TABLE,
    /** A view implementing a term. */
    VIEW,
    /** A table column implementing a term. */
    COLUMN,
    /** A fileset implementing a term. */
    FILESET,
    /** A topic implementing a term. */
    TOPIC,
    /** A model implementing a term. */
    MODEL,
    /** A semantic-model metric using a term. */
    METRIC;

    /** Maps an Implemented-by {@link MetadataObject.Type} to its binding target type. */
    public static TargetType fromMetadataObjectType(MetadataObject.Type type) {
      Preconditions.checkArgument(type != null, "Metadata object type cannot be null");
      return switch (type) {
        case CATALOG -> CATALOG;
        case SCHEMA -> SCHEMA;
        case TABLE -> TABLE;
        case VIEW -> VIEW;
        case COLUMN -> COLUMN;
        case FILESET -> FILESET;
        case TOPIC -> TOPIC;
        case MODEL -> MODEL;
        default -> throw new IllegalArgumentException(
            "Glossary terms cannot be bound to metadata object type: " + type);
      };
    }

    /** Returns the corresponding {@link MetadataObject.Type} for Implemented-by target types. */
    public MetadataObject.Type toMetadataObjectType() {
      Preconditions.checkState(this != METRIC, "METRIC is not a MetadataObject.Type");
      return MetadataObject.Type.valueOf(name());
    }
  }

  /** One retired binding recorded in a surviving term's cleanup audit entry. */
  public record LostBinding(
      TargetType targetType,
      long targetId,
      @Nullable String targetName,
      @Nullable Long containerId,
      @Nullable String containerName) {}

  /** Audit entry emitted after commit for one surviving term that lost one or more bindings. */
  public record BindingLossAuditRecord(
      long metalakeId,
      long termId,
      String termName,
      long resultingRevision,
      String triggeringOperation,
      List<LostBinding> lostBindings) {}

  /** Receives committed binding-loss audit records. */
  @FunctionalInterface
  public interface CleanupAuditListener {
    /** Called after the cleanup transaction commits. */
    void onBindingsCleaned(BindingLossAuditRecord record);
  }

  private final List<CleanupAuditListener> listeners = new CopyOnWriteArrayList<>();

  private GlossaryTermBindingService() {}

  /** Returns the shared binding storage service. */
  public static GlossaryTermBindingService getInstance() {
    return INSTANCE;
  }

  /** Registers a listener for committed binding-loss audit records. */
  public void registerCleanupAuditListener(CleanupAuditListener listener) {
    Preconditions.checkArgument(listener != null, "listener cannot be null");
    listeners.add(listener);
  }

  /** Unregisters a previously registered cleanup audit listener. */
  public void unregisterCleanupAuditListener(CleanupAuditListener listener) {
    listeners.remove(listener);
  }

  /** Inserts a live binding row. The caller holds the endpoint and term locks. */
  public void add(long metalakeId, long termId, TargetType targetType, long targetId) {
    Preconditions.checkArgument(targetType != null, "targetType cannot be null");
    SessionUtils.doWithoutCommit(
        GlossaryTermBindingMapper.class,
        mapper ->
            Preconditions.checkState(
                mapper.insert(metalakeId, termId, targetType.name(), targetId) == 1,
                "Failed to add binding for term %s to %s:%s",
                termId,
                targetType,
                targetId));
  }

  /** Retires one live binding row if present. The caller holds the endpoint and term locks. */
  public boolean remove(long termId, TargetType targetType, long targetId) {
    Preconditions.checkArgument(targetType != null, "targetType cannot be null");
    return SessionUtils.getWithoutCommit(
        GlossaryTermBindingMapper.class,
        mapper ->
            mapper.remove(
                    termId,
                    targetType.name(),
                    targetId,
                    nextDeletedAt(mapper.lastDeletedAt(termId)))
                == 1);
  }

  /** Lists live term IDs bound to a single target. */
  public Set<Long> listTermIds(long metalakeId, TargetType targetType, long targetId) {
    Preconditions.checkArgument(targetType != null, "targetType cannot be null");
    return new LinkedHashSet<>(
        SessionUtils.getWithoutCommit(
            GlossaryTermBindingMapper.class,
            mapper -> mapper.listTermIds(metalakeId, targetType.name(), targetId)));
  }

  /** Lists live bindings for a collection of target IDs of one type, in bounded batches. */
  public List<GlossaryTermBindingPO> listByTargets(
      long metalakeId, TargetType targetType, Collection<Long> targetIds) {
    Preconditions.checkArgument(targetType != null, "targetType cannot be null");
    if (targetIds.isEmpty()) {
      return List.of();
    }
    List<GlossaryTermBindingPO> result = new ArrayList<>();
    SessionUtils.doWithoutCommit(
        GlossaryTermBindingMapper.class,
        mapper -> {
          for (List<Long> batch : Lists.partition(List.copyOf(targetIds), BATCH_SIZE)) {
            result.addAll(mapper.listByTargets(metalakeId, targetType.name(), batch));
          }
        });
    return result;
  }

  /** Lists live bindings of a term in keyset order on (target_type, target_id). */
  public List<GlossaryTermBindingPO> listByTerm(
      long metalakeId, long termId, @Nullable Set<TargetType> targetTypes) {
    List<String> typeNames =
        targetTypes == null || targetTypes.isEmpty()
            ? null
            : targetTypes.stream().map(TargetType::name).toList();
    return SessionUtils.getWithoutCommit(
        GlossaryTermBindingMapper.class,
        mapper -> mapper.listByTerm(metalakeId, termId, typeNames));
  }

  /** Retires every binding row of a deleted term inside its deletion transaction. */
  public void removeTerm(long termId) {
    SessionUtils.doWithoutCommit(
        GlossaryTermBindingMapper.class,
        mapper -> mapper.removeTerm(termId, nextDeletedAt(mapper.lastDeletedAt(termId))));
  }

  /** Retires every binding row of a dropped metalake inside its deletion transaction. */
  public void dropMetalake(long metalakeId) {
    SessionUtils.doWithoutCommit(
        GlossaryTermBindingMapper.class,
        mapper ->
            mapper.dropMetalake(
                metalakeId, nextDeletedAt(mapper.lastDeletedAtInMetalake(metalakeId))));
  }

  /**
   * Cleans up live bindings for a single dropped metadata object (e.g. catalog, schema, view,
   * fileset, topic, model).
   */
  public void cleanupObjectBindings(
      long metalakeId,
      TargetType targetType,
      long targetId,
      @Nullable String targetName,
      String triggeringOperation) {
    try {
      List<GlossaryTermBindingPO> live = listByTargets(metalakeId, targetType, List.of(targetId));
      if (live.isEmpty()) {
        return;
      }
      Map<String, LostBinding> metadata =
          Map.of(
              bindingKey(targetType.name(), targetId),
              new LostBinding(targetType, targetId, targetName, null, null));
      cleanupBindings(metalakeId, live, metadata, triggeringOperation);
    } catch (RuntimeException e) {
      logCleanupFailure(triggeringOperation, metalakeId, targetType + ":" + targetId, e);
      throw e;
    }
  }

  /**
   * Cleans up live bindings for a dropped table and all of its live columns in the caller's table
   * deletion transaction, before column rows are soft-deleted.
   */
  public void cleanupTableBindings(
      long metalakeId, long tableId, @Nullable String tableName, String triggeringOperation) {
    List<GlossaryTermBindingPO> live =
        collectTableBindings(metalakeId, tableId, triggeringOperation);
    if (live.isEmpty()) {
      return;
    }
    retireCollectedBindings(
        metalakeId, live, tableBindingMetadata(tableId, tableName, live), triggeringOperation);
  }

  /**
   * Collects live bindings for a table and all of its live columns without acquiring glossary term
   * locks, so callers that must still acquire additional table/column locks can defer term locking
   * until all endpoint locks are held.
   */
  public List<GlossaryTermBindingPO> collectTableBindings(
      long metalakeId, long tableId, String triggeringOperation) {
    try {
      return SessionUtils.getWithoutCommit(
          GlossaryTermBindingMapper.class, mapper -> mapper.listByTable(metalakeId, tableId));
    } catch (RuntimeException e) {
      logCleanupFailure(triggeringOperation, metalakeId, "TABLE:" + tableId, e);
      throw e;
    }
  }

  /** Builds {@link LostBinding} metadata keyed by target type and ID for table/column bindings. */
  public Map<String, LostBinding> tableBindingMetadata(
      long tableId, @Nullable String tableName, List<GlossaryTermBindingPO> bindings) {
    Map<String, LostBinding> metadata = new LinkedHashMap<>();
    for (GlossaryTermBindingPO row : bindings) {
      TargetType type = TargetType.valueOf(row.getTargetType());
      String key = bindingKey(row.getTargetType(), row.getTargetId());
      if (type == TargetType.TABLE) {
        metadata.putIfAbsent(key, new LostBinding(type, row.getTargetId(), tableName, null, null));
      } else {
        metadata.putIfAbsent(
            key, new LostBinding(type, row.getTargetId(), null, tableId, tableName));
      }
    }
    return metadata;
  }

  /**
   * Collects live bindings for removed columns of a table without acquiring glossary term locks.
   */
  public List<GlossaryTermBindingPO> collectColumnBindings(
      long metalakeId,
      long tableId,
      Collection<Long> removedColumnIds,
      String triggeringOperation) {
    if (removedColumnIds.isEmpty()) {
      return List.of();
    }
    try {
      return listByTargets(metalakeId, TargetType.COLUMN, removedColumnIds);
    } catch (RuntimeException e) {
      logCleanupFailure(
          triggeringOperation, metalakeId, "TABLE:" + tableId + "/COLUMNS:" + removedColumnIds, e);
      throw e;
    }
  }

  /** Builds {@link LostBinding} metadata keyed by target type and ID for removed columns. */
  public Map<String, LostBinding> columnBindingMetadata(
      long tableId, @Nullable String tableName, Map<Long, String> removedColumnNamesById) {
    Map<String, LostBinding> metadata = new LinkedHashMap<>();
    for (Map.Entry<Long, String> entry : removedColumnNamesById.entrySet()) {
      metadata.put(
          bindingKey(TargetType.COLUMN.name(), entry.getKey()),
          new LostBinding(TargetType.COLUMN, entry.getKey(), entry.getValue(), tableId, tableName));
    }
    return metadata;
  }

  /**
   * Cleans up live bindings for removed columns of a table inside the table update or column
   * reconciliation transaction.
   */
  public void cleanupColumnBindings(
      long metalakeId,
      long tableId,
      @Nullable String tableName,
      Map<Long, String> removedColumnNamesById,
      String triggeringOperation) {
    if (removedColumnNamesById.isEmpty()) {
      return;
    }
    try {
      List<GlossaryTermBindingPO> live =
          listByTargets(metalakeId, TargetType.COLUMN, removedColumnNamesById.keySet());
      if (live.isEmpty()) {
        return;
      }
      Map<String, LostBinding> metadata = new LinkedHashMap<>();
      for (Map.Entry<Long, String> entry : removedColumnNamesById.entrySet()) {
        metadata.put(
            bindingKey(TargetType.COLUMN.name(), entry.getKey()),
            new LostBinding(
                TargetType.COLUMN, entry.getKey(), entry.getValue(), tableId, tableName));
      }
      cleanupBindings(metalakeId, live, metadata, triggeringOperation);
    } catch (RuntimeException e) {
      logCleanupFailure(
          triggeringOperation,
          metalakeId,
          "TABLE:" + tableId + "/COLUMNS:" + removedColumnNamesById.keySet(),
          e);
      throw e;
    }
  }

  /**
   * Cleans up live bindings for retired metrics of a semantic model inside the alter transaction.
   */
  public void cleanupMetricBindings(
      long metalakeId,
      long semanticModelId,
      @Nullable String semanticModelName,
      Map<Long, String> retiredMetricNamesById,
      String triggeringOperation) {
    if (retiredMetricNamesById.isEmpty()) {
      return;
    }
    try {
      List<GlossaryTermBindingPO> live =
          listByTargets(metalakeId, TargetType.METRIC, retiredMetricNamesById.keySet());
      if (live.isEmpty()) {
        return;
      }
      Map<String, LostBinding> metadata = new LinkedHashMap<>();
      for (Map.Entry<Long, String> entry : retiredMetricNamesById.entrySet()) {
        metadata.put(
            bindingKey(TargetType.METRIC.name(), entry.getKey()),
            new LostBinding(
                TargetType.METRIC,
                entry.getKey(),
                entry.getValue(),
                semanticModelId,
                semanticModelName));
      }
      cleanupBindings(metalakeId, live, metadata, triggeringOperation);
    } catch (RuntimeException e) {
      logCleanupFailure(
          triggeringOperation,
          metalakeId,
          "SEMANTIC_MODEL:" + semanticModelId + "/METRICS:" + retiredMetricNamesById.keySet(),
          e);
      throw e;
    }
  }

  /**
   * Cleans up live bindings for all live metrics of a dropped semantic model, before metric rows
   * are soft-deleted.
   */
  public void cleanupSemanticModelBindings(
      long metalakeId,
      long semanticModelId,
      @Nullable String semanticModelName,
      String triggeringOperation) {
    try {
      List<GlossaryTermBindingPO> live =
          SessionUtils.getWithoutCommit(
              GlossaryTermBindingMapper.class,
              mapper -> mapper.listBySemanticModel(metalakeId, semanticModelId));
      if (live.isEmpty()) {
        return;
      }
      Map<String, LostBinding> metadata = new LinkedHashMap<>();
      for (GlossaryTermBindingPO row : live) {
        metadata.putIfAbsent(
            bindingKey(row.getTargetType(), row.getTargetId()),
            new LostBinding(
                TargetType.METRIC, row.getTargetId(), null, semanticModelId, semanticModelName));
      }
      cleanupBindings(metalakeId, live, metadata, triggeringOperation);
    } catch (RuntimeException e) {
      logCleanupFailure(triggeringOperation, metalakeId, "SEMANTIC_MODEL:" + semanticModelId, e);
      throw e;
    }
  }

  /**
   * Collects live bindings for schemas and all their live descendant objects and metrics inside a
   * cascading schema drop, without acquiring glossary term locks.
   *
   * <p>Callers must invoke this after soft-deleting the schema rows (excluding new bindings) and
   * before soft-deleting child entity rows, then pass the returned list to {@link
   * #retireCollectedBindings} after all child entity rows are soft-deleted so term locks are always
   * acquired after endpoint locks.
   */
  public List<GlossaryTermBindingPO> collectSchemaCascadeBindings(
      long metalakeId, Collection<Long> schemaIds, String triggeringOperation) {
    if (schemaIds.isEmpty()) {
      return List.of();
    }
    try {
      List<GlossaryTermBindingPO> live = new ArrayList<>();
      SessionUtils.doWithoutCommit(
          GlossaryTermBindingMapper.class,
          mapper -> {
            for (List<Long> batch : Lists.partition(List.copyOf(schemaIds), BATCH_SIZE)) {
              live.addAll(mapper.listBySchemas(metalakeId, batch));
            }
          });
      return live;
    } catch (RuntimeException e) {
      logCleanupFailure(triggeringOperation, metalakeId, "SCHEMAS:" + schemaIds, e);
      throw e;
    }
  }

  /**
   * Collects live bindings for a catalog and all its live descendant objects and metrics inside a
   * cascading catalog drop, without acquiring glossary term locks.
   *
   * <p>Callers must invoke this after soft-deleting the catalog row (excluding new bindings) and
   * before soft-deleting child schemas and entities, then pass the returned list to {@link
   * #retireCollectedBindings} after all child rows are soft-deleted so term locks are always
   * acquired after endpoint locks.
   */
  public List<GlossaryTermBindingPO> collectCatalogCascadeBindings(
      long metalakeId, long catalogId, String triggeringOperation) {
    try {
      return SessionUtils.getWithoutCommit(
          GlossaryTermBindingMapper.class, mapper -> mapper.listByCatalog(metalakeId, catalogId));
    } catch (RuntimeException e) {
      logCleanupFailure(triggeringOperation, metalakeId, "CATALOG:" + catalogId, e);
      throw e;
    }
  }

  /**
   * Retires bindings previously collected during a cascading drop after all child endpoint locks
   * have been acquired. Cascade callers do not resolve display names for every deleted descendant,
   * so {@link LostBinding} entries record target type and target ID with null names.
   */
  public void retireCollectedBindings(
      long metalakeId, List<GlossaryTermBindingPO> bindings, String triggeringOperation) {
    retireCollectedBindings(metalakeId, bindings, Map.of(), triggeringOperation);
  }

  /**
   * Retires previously collected bindings with optional target display metadata after all endpoint
   * locks have been acquired.
   */
  public void retireCollectedBindings(
      long metalakeId,
      List<GlossaryTermBindingPO> bindings,
      Map<String, LostBinding> metadataByKey,
      String triggeringOperation) {
    if (bindings.isEmpty()) {
      return;
    }
    try {
      cleanupBindings(metalakeId, bindings, metadataByKey, triggeringOperation);
    } catch (RuntimeException e) {
      logCleanupFailure(triggeringOperation, metalakeId, "COLLECTED:" + bindings.size(), e);
      throw e;
    }
  }

  /** Purges at most limit retired rows older than cutoff, in the caller's transaction. */
  public int collect(long cutoff, int limit) {
    Preconditions.checkArgument(limit >= 0, "limit must be nonnegative");
    if (limit == 0) {
      return 0;
    }
    return SessionUtils.getWithoutCommit(
        GlossaryTermBindingMapper.class,
        mapper -> {
          int count = 0;
          for (GlossaryTermBindingPO row : mapper.expired(cutoff, limit)) {
            count += mapper.purge(row);
          }
          return count;
        });
  }

  private void cleanupBindings(
      long metalakeId,
      List<GlossaryTermBindingPO> bindings,
      Map<String, LostBinding> metadataByKey,
      String triggeringOperation) {
    // Group by ascending term ID so term rows are always locked in global ID order.
    Map<Long, List<GlossaryTermBindingPO>> byTerm = new TreeMap<>();
    for (GlossaryTermBindingPO row : bindings) {
      byTerm.computeIfAbsent(row.getTermId(), ignored -> new ArrayList<>()).add(row);
    }

    // Lock all affected surviving term rows in ascending ID order before retiring bindings.
    Map<Long, GlossaryTermPO> lockedTerms = new LinkedHashMap<>();
    SessionUtils.doWithoutCommit(
        GlossaryTermMetaMapper.class,
        termMapper -> {
          for (long termId : byTerm.keySet()) {
            GlossaryTermPO locked = termMapper.getByIdForUpdate(metalakeId, termId);
            if (locked != null) {
              lockedTerms.put(termId, locked);
            }
          }
        });

    // Soft-delete each matching live binding row after locking the affected surviving terms.
    // Track only the rows retired by this transaction so concurrent cleanups cannot double-bump
    // a term's revision or emit duplicate audit records.
    Map<Long, List<GlossaryTermBindingPO>> actuallyRemovedByTerm = new LinkedHashMap<>();
    SessionUtils.doWithoutCommit(
        GlossaryTermBindingMapper.class,
        bindingMapper -> {
          long deletedAt = nextDeletedAt(bindingMapper.lastDeletedAtInMetalake(metalakeId));
          for (Map.Entry<Long, List<GlossaryTermBindingPO>> entry : byTerm.entrySet()) {
            long termId = entry.getKey();
            for (GlossaryTermBindingPO row : entry.getValue()) {
              int removed =
                  bindingMapper.remove(termId, row.getTargetType(), row.getTargetId(), deletedAt);
              if (removed == 1 && lockedTerms.containsKey(termId)) {
                actuallyRemovedByTerm
                    .computeIfAbsent(termId, ignored -> new ArrayList<>())
                    .add(row);
              }
            }
          }
        });

    // Increment each affected surviving term's revision once and stage its post-commit audit
    // entry.
    List<BindingLossAuditRecord> records = new ArrayList<>(actuallyRemovedByTerm.size());
    SessionUtils.doWithoutCommit(
        GlossaryTermMetaMapper.class,
        termMapper -> {
          for (Map.Entry<Long, List<GlossaryTermBindingPO>> entry :
              actuallyRemovedByTerm.entrySet()) {
            long termId = entry.getKey();
            GlossaryTermPO term = lockedTerms.get(termId);
            int updated = termMapper.incrementVersion(metalakeId, termId, term.getCurrentVersion());
            Preconditions.checkState(
                updated == 1, "Failed to increment version for locked glossary term %s", termId);
            long newRevision = Math.addExact(term.getCurrentVersion(), 1L);
            List<LostBinding> lost = new ArrayList<>(entry.getValue().size());
            for (GlossaryTermBindingPO row : entry.getValue()) {
              String key = bindingKey(row.getTargetType(), row.getTargetId());
              LostBinding detail = metadataByKey.get(key);
              if (detail == null) {
                detail =
                    new LostBinding(
                        TargetType.valueOf(row.getTargetType()),
                        row.getTargetId(),
                        null,
                        null,
                        null);
              }
              lost.add(detail);
            }
            records.add(
                new BindingLossAuditRecord(
                    metalakeId,
                    termId,
                    term.getName(),
                    newRevision,
                    triggeringOperation,
                    List.copyOf(lost)));
          }
        });

    if (!records.isEmpty()) {
      SqlSessions.afterCommit(
          () -> {
            for (BindingLossAuditRecord record : records) {
              LOG.info(
                  "Cleaned up {} glossary binding(s) for term {} (id={}) on {}: new revision={}",
                  record.lostBindings().size(),
                  record.termName(),
                  record.termId(),
                  record.triggeringOperation(),
                  record.resultingRevision());
              for (CleanupAuditListener listener : listeners) {
                try {
                  listener.onBindingsCleaned(record);
                } catch (RuntimeException e) {
                  LOG.warn(
                      "CleanupAuditListener failed for term {} (id={}) on {}",
                      record.termName(),
                      record.termId(),
                      record.triggeringOperation(),
                      e);
                }
              }
            }
          });
    }
  }

  private static void logCleanupFailure(
      String triggeringOperation, long metalakeId, String targetRef, RuntimeException cause) {
    LOG.error(
        "Glossary binding cleanup failed for operation {} in metalake {} target={}",
        triggeringOperation,
        metalakeId,
        targetRef,
        cause);
  }

  private static String bindingKey(String targetType, long targetId) {
    return targetType + ":" + targetId;
  }

  private static long nextDeletedAt(long lastDeletedAt) {
    return Math.max(Math.max(1L, System.currentTimeMillis()), Math.addExact(lastDeletedAt, 1L));
  }
}
