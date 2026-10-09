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

import static org.apache.gravitino.metrics.source.MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME;

import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.meta.StatisticEntity;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.StatisticMetaMapper;
import org.apache.gravitino.storage.relational.po.StatisticPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * The service class for statistic metadata. It provides the basic database operations for
 * statistic.
 */
public class StatisticMetaService {

  private static final StatisticMetaService INSTANCE = new StatisticMetaService();

  public static StatisticMetaService getInstance() {
    return INSTANCE;
  }

  @VisibleForTesting
  StatisticMetaService() {}

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listStatisticsByEntity")
  public List<StatisticEntity> listStatisticsByEntity(
      NameIdentifier identifier, Entity.EntityType type) {
    NamespacedEntityId namespacedEntityId = EntityIdService.getEntityIds(identifier, type);
    List<StatisticPO> statisticPOs =
        SessionUtils.getWithoutCommit(
            StatisticMetaMapper.class,
            mapper ->
                mapper.listStatisticPOsByEntityId(
                    namespacedEntityId.namespaceIds()[0], namespacedEntityId.entityId()));
    return statisticPOs.stream()
        .map(po -> StatisticPO.fromStatisticPO(po, identifier))
        .collect(Collectors.toList());
  }

  /**
   * Creates or replaces statistics of a metadata object by name.
   *
   * <p>Existing statistics are replaced only if their version is unchanged since this call read
   * them, and missing statistics are inserted only if nobody created them meanwhile; both cases
   * otherwise fail the whole batch with {@link OptimisticLockException}. The target is fenced in
   * the same transaction, so a target dropped or replaced after its ID was resolved fails with
   * {@link NoSuchEntityException}.
   *
   * @param statisticEntities the statistics to write; names must be unique in the batch
   * @param entity the metadata object that owns the statistics
   * @param type the metadata object type
   */
  // Preserve the historical metric name for compatibility with existing monitoring.
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "batchInsertStatisticPOsOnDuplicateKeyUpdate")
  public void writeStatisticsWithVersion(
      List<StatisticEntity> statisticEntities, NameIdentifier entity, Entity.EntityType type) {
    if (statisticEntities == null || statisticEntities.isEmpty()) {
      return;
    }

    NamespacedEntityId namespacedEntityId = EntityIdService.getEntityIds(entity, type);
    List<StatisticPO> pos =
        StatisticPO.initializeStatisticPOs(
            statisticEntities,
            namespacedEntityId.namespaceIds()[0],
            namespacedEntityId.entityId(),
            NameIdentifierUtil.toMetadataObject(entity, type).type());
    Set<String> names = new HashSet<>();
    for (StatisticPO po : pos) {
      if (!names.add(po.getStatisticName())) {
        throw new IllegalArgumentException(
            "Duplicate statistic name in batch: " + po.getStatisticName());
      }
    }
    pos.sort(Comparator.comparing(StatisticPO::getStatisticName));
    Map<String, StatisticPO> previous =
        listStatisticPOs(
                namespacedEntityId,
                pos.stream().map(StatisticPO::getStatisticName).collect(Collectors.toList()))
            .stream()
            .collect(Collectors.toMap(StatisticPO::getStatisticName, Function.identity()));
    List<StatisticPO> inserts = new ArrayList<>();
    List<StatisticPO> updates = new ArrayList<>();
    for (StatisticPO po : pos) {
      StatisticPO old = previous.get(po.getStatisticName());
      if (old == null) {
        inserts.add(po);
      } else {
        updates.add(replacementOf(old, po));
      }
    }
    // Each kind of write is one statement. Any mismatch fails the whole transaction, so the batch
    // either applies completely or not at all.
    SessionUtils.doMultipleWithCommit(
        () -> {
          LiveEndpointService.lockLiveEndpoint(entity, type, namespacedEntityId);
          if (!inserts.isEmpty()) {
            executeCas(
                mapper -> mapper.batchInsertStatisticPOs(inserts), inserts.size(), inserts, entity);
          }
          if (!updates.isEmpty()) {
            executeCas(
                mapper -> mapper.batchUpdateStatisticPOsWithVersion(updates),
                updates.size(),
                updates,
                entity);
          }
        });
  }

  /**
   * Soft-deletes the named statistics of a metadata object.
   *
   * <p>Each statistic is deleted only at the version this call read. A statistic that was changed
   * meanwhile fails the whole batch with {@link OptimisticLockException}; one that a concurrent
   * drop already removed is not counted, like a name that does not exist. A same-name replacement
   * is a conflict even if its version matches the deleted row. A target dropped or replaced after
   * its ID was resolved fails with {@link NoSuchEntityException} when there are rows to delete.
   *
   * @param identifier the metadata object that owns the statistics
   * @param type the metadata object type
   * @param statisticNames the statistic names to delete
   * @return the number of statistics this call deleted
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "batchDeleteStatisticPOs")
  public int batchDeleteStatisticPOs(
      NameIdentifier identifier, Entity.EntityType type, List<String> statisticNames) {
    if (statisticNames == null || statisticNames.isEmpty()) {
      return 0;
    }
    NamespacedEntityId observed = EntityIdService.getEntityIds(identifier, type);
    Set<String> orderedNames =
        statisticNames.stream()
            .filter(Objects::nonNull)
            .collect(Collectors.toCollection(TreeSet::new));
    if (orderedNames.isEmpty()) {
      return 0;
    }
    Map<String, StatisticPO> previous =
        listStatisticPOs(observed, new ArrayList<>(orderedNames)).stream()
            .collect(Collectors.toMap(StatisticPO::getStatisticName, Function.identity()));
    if (previous.isEmpty()) {
      return 0;
    }
    // Look up by the exact requested names. A collation that ignores trailing spaces can return the
    // row of "name" for a request of "name "; dropping "name " must not drop "name".
    List<StatisticPO> observedRows =
        orderedNames.stream()
            .map(previous::get)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    if (observedRows.isEmpty()) {
      return 0;
    }
    int[] deleted = new int[] {0};
    SessionUtils.doMultipleWithCommit(
        () -> {
          LiveEndpointService.lockLiveEndpoint(identifier, type, observed);
          try {
            deleted[0] =
                SessionUtils.getWithoutCommit(
                    StatisticMetaMapper.class,
                    mapper -> mapper.batchDeleteStatisticPOsWithVersion(observedRows));
          } catch (RuntimeException e) {
            throw translateConflict(e, observedRows, identifier);
          }
          if (deleted[0] == observedRows.size()) {
            return;
          }
          // Some observed rows were not deleted. A name that is still live was changed or
          // replaced meanwhile, which is a conflict. Otherwise a concurrent drop already removed
          // it; like a drop of a missing name, that is not a conflict and is simply not counted.
          List<StatisticPO> live = liveStatistics(observed, names(observedRows));
          if (!live.isEmpty()) {
            throw statisticConflict(null, names(live), identifier);
          }
        });
    return deleted[0];
  }

  @VisibleForTesting
  List<StatisticPO> listStatisticPOs(NamespacedEntityId endpoint, List<String> names) {
    return SessionUtils.getWithoutCommit(
        StatisticMetaMapper.class,
        mapper ->
            mapper.listStatisticPOsByNames(endpoint.namespaceIds()[0], endpoint.entityId(), names));
  }

  /**
   * Runs one batch statement that must change exactly {@code expected} rows, and reports a short
   * count or a concurrency failure of the statement as a conflict.
   */
  @VisibleForTesting
  static void executeCas(
      Function<StatisticMetaMapper, Integer> statement,
      int expected,
      List<StatisticPO> pos,
      NameIdentifier target) {
    int changed;
    try {
      changed = SessionUtils.getWithoutCommit(StatisticMetaMapper.class, statement);
    } catch (RuntimeException e) {
      throw translateConflict(e, pos, target);
    }
    if (changed != expected) {
      throw statisticConflict(null, names(pos), target);
    }
  }

  /**
   * Converts a failure caused by a concurrent writer into a conflict. A duplicate insert means a
   * statistic was created after the snapshot. A deadlock or serialization failure can happen
   * because a batch statement locks its rows in the order of the database's plan, which differs
   * between batches. The whole batch is rolled back either way, so both are reported as a conflict
   * rather than an internal server error.
   */
  private static RuntimeException translateConflict(
      RuntimeException failure, List<StatisticPO> pos, NameIdentifier target) {
    if (isDuplicateKey(failure) || isConcurrencyFailure(failure)) {
      return statisticConflict(failure, names(pos), target);
    }
    return failure;
  }

  private static OptimisticLockException statisticConflict(
      @Nullable Throwable cause, List<String> names, NameIdentifier target) {
    return new OptimisticLockException(
        cause,
        "One or more of the statistics %s of %s were modified concurrently; retry the operation",
        names,
        target);
  }

  private static List<StatisticPO> liveStatistics(NamespacedEntityId endpoint, List<String> names) {
    return SessionUtils.getWithoutCommit(
        StatisticMetaMapper.class,
        mapper ->
            mapper.listStatisticPOsByNames(endpoint.namespaceIds()[0], endpoint.entityId(), names));
  }

  private static List<String> names(List<StatisticPO> pos) {
    return pos.stream().map(StatisticPO::getStatisticName).sorted().collect(Collectors.toList());
  }

  /** Returns a PO that identifies the observed row and carries the replacement value. */
  private static StatisticPO replacementOf(StatisticPO observed, StatisticPO replacement) {
    return StatisticPO.builder()
        .withStatisticId(observed.getStatisticId())
        .withStatisticName(observed.getStatisticName())
        .withMetalakeId(observed.getMetalakeId())
        .withMetadataObjectId(observed.getMetadataObjectId())
        .withMetadataObjectType(observed.getMetadataObjectType())
        .withCurrentVersion(observed.getCurrentVersion())
        .withLastVersion(observed.getLastVersion())
        .withDeletedAt(observed.getDeletedAt())
        .withStatisticValue(replacement.getStatisticValue())
        .withAuditInfo(replacement.getAuditInfo())
        .build();
  }

  /**
   * Returns whether a failure is a unique-key violation. PostgreSQL and H2 report SQLState 23505,
   * and MySQL reports error code 1062, the same codes the backend exception converters match.
   */
  private static boolean isDuplicateKey(Throwable failure) {
    return hasSqlException(failure, Set.of("23505"), 1062);
  }

  /**
   * Returns whether a failure is a deadlock or serialization failure. MySQL and H2 report SQLState
   * 40001 (MySQL error code 1213 for a deadlock), and PostgreSQL reports 40001 or 40P01.
   */
  private static boolean isConcurrencyFailure(Throwable failure) {
    return hasSqlException(failure, Set.of("40001", "40P01"), 1213);
  }

  private static boolean hasSqlException(
      Throwable failure, Set<String> sqlStates, int mysqlErrorCode) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof SQLException) {
        SQLException sql = (SQLException) cause;
        if (sqlStates.contains(sql.getSQLState()) || sql.getErrorCode() == mysqlErrorCode) {
          return true;
        }
      }
    }
    return false;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteStatisticsByLegacyTimeline")
  public int deleteStatisticsByLegacyTimeline(long legacyTimeline, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        StatisticMetaMapper.class,
        mapper -> mapper.deleteStatisticsByLegacyTimeline(legacyTimeline, limit));
  }
}
