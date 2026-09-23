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

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
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

  private StatisticMetaService() {}

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
        listStatisticPOs(namespacedEntityId).stream()
            .collect(Collectors.toMap(StatisticPO::getStatisticName, Function.identity()));
    SessionUtils.doMultipleWithCommit(
        () -> {
          LiveEndpointService.lockLiveEndpoint(entity, type, namespacedEntityId);
          for (StatisticPO po : pos) {
            StatisticPO old = previous.get(po.getStatisticName());
            int updated =
                SessionUtils.getWithoutCommit(
                    StatisticMetaMapper.class,
                    mapper ->
                        old == null
                            ? mapper.insertStatisticPO(po)
                            : mapper.updateStatisticPOWithVersion(po, old));
            if (updated != 1) {
              throw new OptimisticLockException(
                  "Statistic %s for %s changed during update", po.getStatisticName(), entity);
            }
          }
        });
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "batchDeleteStatisticPOs")
  public int batchDeleteStatisticPOs(
      NameIdentifier identifier, Entity.EntityType type, List<String> statisticNames) {
    if (statisticNames == null || statisticNames.isEmpty()) {
      return 0;
    }
    NamespacedEntityId observed = EntityIdService.getEntityIds(identifier, type);
    Map<String, StatisticPO> previous =
        listStatisticPOs(observed).stream()
            .collect(Collectors.toMap(StatisticPO::getStatisticName, Function.identity()));
    Set<String> orderedNames = new TreeSet<>(Comparator.nullsFirst(Comparator.naturalOrder()));
    orderedNames.addAll(statisticNames);
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
            if (updated != 1) {
              throw new OptimisticLockException(
                  "Statistic %s for %s changed during deletion", name, identifier);
            }
            deleted[0]++;
          }
        });
    return deleted[0];
  }

  private List<StatisticPO> listStatisticPOs(NamespacedEntityId endpoint) {
    return SessionUtils.getWithoutCommit(
        StatisticMetaMapper.class,
        mapper ->
            mapper.listStatisticPOsByEntityId(endpoint.namespaceIds()[0], endpoint.entityId()));
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
