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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.StatisticEntity;
import org.apache.gravitino.storage.relational.mapper.StatisticMetaMapper;
import org.apache.gravitino.storage.relational.po.StatisticPO;
import org.apache.gravitino.storage.relational.utils.POConverters;
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

  public List<StatisticEntity> listStatisticsByObject(
      NameIdentifier identifier, Entity.EntityType type) {
    long metalakeId =
        MetalakeMetaService.getInstance()
            .getMetalakeIdByName(NameIdentifierUtil.getMetalake(identifier));
    MetadataObject object = NameIdentifierUtil.toMetadataObject(identifier, type);
    long objectId =
        MetadataObjectService.getMetadataObjectId(metalakeId, object.fullName(), object.type());
    List<StatisticPO> statisticPOs =
        SessionUtils.getWithoutCommit(
            StatisticMetaMapper.class, mapper -> mapper.listStatisticPOsByObjectId(objectId));
    return statisticPOs.stream().map(POConverters::fromStatisticPO).collect(Collectors.toList());
  }

  public void batchInsertStatisticPOs(
      List<StatisticEntity> statisticEntities,
      String metalake,
      NameIdentifier entity,
      Entity.EntityType type) {
    if (statisticEntities == null || statisticEntities.isEmpty()) {
      return;
    }
    Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
    Long objectId;
    MetadataObject object = NameIdentifierUtil.toMetadataObject(entity, type);
    if (type == Entity.EntityType.METALAKE) {
      objectId = metalakeId;
    } else {
      objectId =
          MetadataObjectService.getMetadataObjectId(metalakeId, object.fullName(), object.type());
    }
    StatisticPO.Builder builder = StatisticPO.builder();
    builder.withMetalakeId(metalakeId);
    builder.withMetadataObjectId(objectId);
    builder.withMetadataObjectType(object.type().name());

    List<StatisticPO> pos = POConverters.initializeStatisticPOs(statisticEntities, builder);
    SessionUtils.doWithCommit(
        StatisticMetaMapper.class, mapper -> mapper.batchInsertStatisticPOs(pos));
  }

  public int batchDeleteStatisticPOs(
      String metalake, MetadataObject object, List<String> statisticNames) {
    if (statisticNames == null || statisticNames.isEmpty()) {
      return 0;
    }
    Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
    Long objectId;
    if (object.type() == MetadataObject.Type.METALAKE) {
      objectId = metalakeId;
    } else {
      objectId =
          MetadataObjectService.getMetadataObjectId(metalakeId, object.fullName(), object.type());
    }

    return SessionUtils.doWithCommitAndFetchResult(
        StatisticMetaMapper.class,
        mapper -> mapper.batchDeleteStatisticPOs(objectId, statisticNames));
  }

  public int deleteStatisticsByLegacyTimeline(long legacyTimeline, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        StatisticMetaMapper.class,
        mapper -> mapper.deleteStatisticsByLegacyTimeline(legacyTimeline, limit));
  }
}
