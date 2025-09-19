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

import java.util.Collections;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.po.GroupPO;
import org.apache.gravitino.storage.relational.po.OwnerRelPO;
import org.apache.gravitino.storage.relational.po.UserPO;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** This class is an utilization class to retrieve owner relation. */
public class OwnerMetaService {

  private OwnerMetaService() {}

  private static final OwnerMetaService INSTANCE = new OwnerMetaService();

  public static OwnerMetaService getInstance() {
    return INSTANCE;
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "getOwner")
  public Optional<Entity> getOwner(NameIdentifier identifier, Entity.EntityType type) {
    long metalakeId =
        MetalakeMetaService.getInstance()
            .getMetalakeIdByName(NameIdentifierUtil.getMetalake(identifier));
    Long entityId = getEntityId(metalakeId, identifier, type);

    UserPO userPO =
        SessionUtils.getWithoutCommit(
            OwnerMetaMapper.class,
            mapper -> mapper.selectUserOwnerMetaByMetadataObjectIdAndType(entityId, type.name()));

    if (userPO != null) {
      return Optional.of(
          POConverters.fromUserPO(
              userPO,
              Collections.emptyList(),
              AuthorizationUtils.ofUserNamespace(NameIdentifierUtil.getMetalake(identifier))));
    }

    GroupPO groupPO =
        SessionUtils.getWithoutCommit(
            OwnerMetaMapper.class,
            mapper -> mapper.selectGroupOwnerMetaByMetadataObjectIdAndType(entityId, type.name()));

    if (groupPO != null) {
      return Optional.of(
          POConverters.fromGroupPO(
              groupPO,
              Collections.emptyList(),
              AuthorizationUtils.ofGroupNamespace(NameIdentifierUtil.getMetalake(identifier))));
    }

    return Optional.empty();
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "setOwner")
  public void setOwner(
      NameIdentifier entity,
      Entity.EntityType entityType,
      NameIdentifier owner,
      Entity.EntityType ownerType) {
    long metalakeId =
        MetalakeMetaService.getInstance()
            .getMetalakeIdByName(NameIdentifierUtil.getMetalake(entity));

    Long entityId = getEntityId(metalakeId, entity, entityType);
    Long ownerId = getEntityId(metalakeId, owner, ownerType);

    OwnerRelPO ownerRelPO =
        POConverters.initializeOwnerRelPOsWithVersion(
            metalakeId, ownerType.name(), ownerId, entityType.name(), entityId);
    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class,
                mapper ->
                    mapper.softDeleteOwnerRelByMetadataObjectIdAndType(
                        entityId,
                        NameIdentifierUtil.toMetadataObject(entity, entityType).type().name())),
        () ->
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class, mapper -> mapper.insertOwnerRel(ownerRelPO)));
  }

  private static long getEntityId(
      long metalakeId, NameIdentifier identifier, Entity.EntityType type) {
    switch (type) {
      case USER:
        return UserMetaService.getInstance()
            .getUserIdByMetalakeIdAndName(metalakeId, identifier.name());
      case GROUP:
        return GroupMetaService.getInstance()
            .getGroupIdByMetalakeIdAndName(metalakeId, identifier.name());
      default:
        MetadataObject object = NameIdentifierUtil.toMetadataObject(identifier, type);
        return MetadataObjectService.getMetadataObjectId(
            metalakeId, object.fullName(), object.type());
    }
  }
}
