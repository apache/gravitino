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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
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
    return statisticPOs.stream().map(StatisticPO::fromStatisticPO).collect(Collectors.toList());
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
    SessionUtils.doWithCommit(
        StatisticMetaMapper.class,
        mapper -> mapper.batchInsertStatisticPOsOnDuplicateKeyUpdate(pos));
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "batchDeleteStatisticPOs")
  public int batchDeleteStatisticPOs(
      NameIdentifier identifier, Entity.EntityType type, List<String> statisticNames) {
    if (statisticNames == null || statisticNames.isEmpty()) {
      return 0;
    }
    Long entityId = EntityIdService.getEntityId(identifier, type);

    return SessionUtils.doWithCommitAndFetchResult(
        StatisticMetaMapper.class,
        mapper -> mapper.batchDeleteStatisticPOs(entityId, statisticNames));
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
