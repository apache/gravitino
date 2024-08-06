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

import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.po.OwnerRelPO;
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

  public Optional<Entity> getOwner(NameIdentifier identifier, Entity.EntityType type) {
    String metalakeName = getMetalake(identifier);
    Long entityId = getEntityId(metalakeName, identifier, type);

    OwnerRelPO ownerRelPO =
        SessionUtils.getWithoutCommit(
            OwnerMetaMapper.class,
            mapper -> mapper.selectOwnerMetaByMetadataObjectIdAndType(entityId, type.name()));

    if (ownerRelPO == null) {
      return Optional.empty();
    }

    switch (Entity.EntityType.valueOf(ownerRelPO.getOwnerType())) {
      case USER:
        return Optional.of(
            UserMetaService.getInstance()
                .getUserById(getMetalake(identifier), ownerRelPO.getOwnerId()));
      case GROUP:
        return Optional.of(
            GroupMetaService.getInstance()
                .getGroupById(getMetalake(identifier), ownerRelPO.getOwnerId()));
      default:
        throw new IllegalArgumentException(
            String.format("Owner type doesn't support %s", ownerRelPO.getOwnerType()));
    }
  }

  public void setOwner(
      NameIdentifier entity,
      Entity.EntityType entityType,
      NameIdentifier owner,
      Entity.EntityType ownerType) {

    String metalakeName = getMetalake(entity);
    Long metalakeId =
        getEntityId(metalakeName, NameIdentifier.of(metalakeName), EntityType.METALAKE);
    Long entityId = getEntityId(metalakeName, entity, entityType);
    Long ownerId = getEntityId(metalakeName, owner, ownerType);

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
      String metalake, NameIdentifier identifier, Entity.EntityType type) {
    switch (type) {
      case USER:
        return UserMetaService.getInstance().getUserIdByNameIdentifier(identifier);
      case GROUP:
        return GroupMetaService.getInstance().getGroupIdByNameIdentifier(identifier);
      default:
        MetadataObject object = NameIdentifierUtil.toMetadataObject(identifier, type);
        return MetadataObjectService.getMetadataObjectId(
            metalake, object.fullName(), object.type());
    }
  }

  private static String getMetalake(NameIdentifier identifier) {
    if (identifier.hasNamespace()) {
      return identifier.namespace().level(0);
    } else {
      return identifier.name();
    }
  }
}
