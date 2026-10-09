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
   * <p>The name keeps the historical upsert name and metric, but the write is no longer a blind
   * upsert. Existing statistics are replaced only if their version is unchanged since this call
   * read them, and missing statistics are inserted only if nobody created them meanwhile; both
   * cases otherwise fail the whole batch with {@link OptimisticLockException}. The target is fenced
   * in the same transaction, so a target dropped or replaced after its ID was resolved fails with
   * {@link NoSuchEntityException}.
   *
   * @param statisticEntities the statistics to write; names must be unique in the batch
   * @param entity the metadata object that owns the statistics
   * @param type the metadata object type
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "batchInsertStatisticPOsOnDuplicateKeyUpdate")
  public void batchInsertStatisticPOsOnDuplicateKeyUpdate(
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
    SessionUtils.doMultipleWithCommit(
        () -> {
          LiveEndpointService.lockLiveEndpoint(entity, type, namespacedEntityId);
          // Execute each CAS separately: a batch executor may report SUCCESS_NO_INFO rather than
          // the per-row counts needed to detect conflicts (for example, MySQL batch rewriting).
          for (StatisticPO po : pos) {
            StatisticPO old = previous.get(po.getStatisticName());
            int updated;
            try {
              updated =
                  SessionUtils.getWithoutCommit(
                      StatisticMetaMapper.class,
                      mapper ->
                          old == null
                              ? mapper.insertStatisticPO(po)
                              : mapper.updateStatisticPOWithVersion(po, old));
            } catch (RuntimeException e) {
              // A writer can create the same statistic after the snapshot above. A duplicate
              // insert is a stale snapshot, not an internal server error.
              if (old == null && isDuplicateKey(e)) {
                throw statisticConflict(e, po.getStatisticName(), entity);
              }
              throw e;
            }
            if (updated != 1) {
              throw statisticConflict(null, po.getStatisticName(), entity);
            }
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
    int[] deleted = new int[] {0};
    SessionUtils.doMultipleWithCommit(
        () -> {
          LiveEndpointService.lockLiveEndpoint(identifier, type, observed);
          for (String name : orderedNames) {
            StatisticPO old = previous.get(name);
            if (old == null) {
              continue;
            }
            int updated =
                SessionUtils.getWithoutCommit(
                    StatisticMetaMapper.class, mapper -> mapper.deleteStatisticPOWithVersion(old));
            if (updated == 1) {
              deleted[0]++;
            } else if (hasLiveStatistic(observed, name)) {
              throw statisticConflict(null, name, identifier);
            }
            // Otherwise a concurrent drop already removed it. Like a drop of a missing name, that
            // is not a conflict and is simply not counted.
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

  private static OptimisticLockException statisticConflict(
      @Nullable Throwable cause, String name, NameIdentifier target) {
    return new OptimisticLockException(
        cause,
        "The statistic %s of %s was modified concurrently; retry the operation",
        name,
        target);
  }

  private static boolean hasLiveStatistic(NamespacedEntityId endpoint, String name) {
    return !SessionUtils.getWithoutCommit(
            StatisticMetaMapper.class,
            mapper ->
                mapper.listStatisticPOsByNames(
                    endpoint.namespaceIds()[0], endpoint.entityId(), List.of(name)))
        .isEmpty();
  }

  /**
   * Returns whether a failure is a unique-key violation. PostgreSQL and H2 report SQLState 23505,
   * and MySQL reports error code 1062, the same codes the backend exception converters match.
   */
  private static boolean isDuplicateKey(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof SQLException) {
        SQLException sql = (SQLException) cause;
        if ("23505".equals(sql.getSQLState()) || sql.getErrorCode() == 1062) {
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
