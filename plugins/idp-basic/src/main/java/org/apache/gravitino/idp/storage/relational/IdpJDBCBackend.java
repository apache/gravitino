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
package org.apache.gravitino.idp.storage.relational;

import static org.apache.gravitino.Configs.GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.UnsupportedEntityTypeException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.idp.authorization.IdpAuthorizationUtils;
import org.apache.gravitino.idp.exception.NotFoundException;
import org.apache.gravitino.idp.meta.IdpEntity;
import org.apache.gravitino.idp.meta.IdpEntityType;
import org.apache.gravitino.idp.meta.IdpGroupEntity;
import org.apache.gravitino.idp.meta.IdpUserEntity;
import org.apache.gravitino.idp.storage.converter.IdpPOConverters;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.gravitino.idp.storage.service.IdpGroupMetaService;
import org.apache.gravitino.idp.storage.service.IdpUserMetaService;
import org.apache.gravitino.storage.relational.JDBCBackend;

/**
 * JDBC backend for built-in IdP entities. It reuses JDBC initialization from {@link JDBCBackend}
 * without modifying core entity types.
 */
public class IdpJDBCBackend extends JDBCBackend {

  /**
   * Lists entities under the namespace.
   *
   * @param namespace The namespace of the entities.
   * @param entityType The built-in IdP entity type.
   * @param allFields Whether to fetch all fields.
   * @param <E> The entity type.
   * @return The entities.
   * @throws IOException If the list operation fails.
   */
  public <E extends IdpEntity & HasIdentifier> List<E> list(
      Namespace namespace, IdpEntityType entityType, boolean allFields) throws IOException {
    throw new UnsupportedOperationException(
        "List is not supported for built-in IdP entity type: " + entityType);
  }

  public boolean exists(NameIdentifier ident, IdpEntityType entityType) throws IOException {
    return entityExists(ident, entityType);
  }

  public <E extends IdpEntity & HasIdentifier> void insert(E entity, boolean overwritten)
      throws EntityAlreadyExistsException, IOException {
    if (entity instanceof IdpUserEntity) {
      insertIdpUser((IdpUserEntity) entity, overwritten);
    } else if (entity instanceof IdpGroupEntity) {
      insertIdpGroup((IdpGroupEntity) entity, overwritten);
    } else {
      throw new UnsupportedEntityTypeException(
          "Unsupported entity type: %s for insert operation", entity.type());
    }
  }

  public <E extends IdpEntity & HasIdentifier> E get(NameIdentifier ident, IdpEntityType entityType)
      throws IOException {
    switch (entityType) {
      case IDP_USER:
        return (E) getIdpUser(ident);
      case IDP_GROUP:
        return (E) getIdpGroup(ident);
      default:
        throw new UnsupportedEntityTypeException(
            "Unsupported entity type: %s for get operation", entityType);
    }
  }

  public <E extends IdpEntity & HasIdentifier> List<E> batchGet(
      List<NameIdentifier> identifiers, IdpEntityType entityType) {
    List<E> entities = new ArrayList<>(identifiers.size());
    for (NameIdentifier identifier : identifiers) {
      try {
        entities.add(get(identifier, entityType));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return entities;
  }

  public boolean delete(NameIdentifier ident, IdpEntityType entityType, boolean cascade)
      throws IOException {
    if (!entityExists(ident, entityType)) {
      return false;
    }
    switch (entityType) {
      case IDP_USER:
        IdpAuthorizationUtils.checkIdpUser(ident);
        return IdpUserMetaService.getInstance().deleteIdpUser(ident.name());
      case IDP_GROUP:
        IdpAuthorizationUtils.checkIdpGroup(ident);
        return IdpGroupMetaService.getInstance().deleteIdpGroup(ident.name());
      default:
        throw new UnsupportedEntityTypeException(
            "Unsupported entity type: %s for delete operation", entityType);
    }
  }

  public int hardDeleteLegacyData(IdpEntityType entityType, long legacyTimeline)
      throws IOException {
    switch (entityType) {
      case IDP_USER:
        return IdpUserMetaService.getInstance()
            .deleteUserMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case IDP_GROUP:
        return IdpGroupMetaService.getInstance()
            .deleteGroupMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      default:
        throw new IllegalArgumentException(
            "Unsupported entity type when collectAndRemoveLegacyData: " + entityType);
    }
  }

  public int deleteOldVersionData(IdpEntityType entityType, long versionRetentionCount)
      throws IOException {
    switch (entityType) {
      case IDP_USER:
      case IDP_GROUP:
        return 0;
      default:
        throw new IllegalArgumentException(
            "Unsupported entity type when collectAndRemoveOldVersionData: " + entityType);
    }
  }

  private static IdpUserEntity getIdpUser(NameIdentifier ident) {
    IdpAuthorizationUtils.checkIdpUser(ident);
    try {
      IdpUserPO userPO = IdpUserMetaService.getInstance().getIdpUserByUsername(ident.name());
      List<String> groupNames =
          IdpUserMetaService.getInstance().listGroupNamesByUsername(ident.name());
      return IdpPOConverters.fromIdpUserPO(userPO, groupNames, ident.namespace());
    } catch (NotFoundException e) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          IdpEntityType.IDP_USER.name().toLowerCase(),
          ident.name());
    }
  }

  private static IdpGroupEntity getIdpGroup(NameIdentifier ident) {
    IdpAuthorizationUtils.checkIdpGroup(ident);
    try {
      IdpGroupPO groupPO = IdpGroupMetaService.getInstance().getIdpGroupByName(ident.name());
      List<String> userNames =
          IdpGroupMetaService.getInstance().listUsernamesByGroupName(ident.name());
      return IdpPOConverters.fromIdpGroupPO(groupPO, userNames, ident.namespace());
    } catch (NotFoundException e) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          IdpEntityType.IDP_GROUP.name().toLowerCase(),
          ident.name());
    }
  }

  private static void insertIdpUser(IdpUserEntity userEntity, boolean overwritten)
      throws EntityAlreadyExistsException {
    IdpAuthorizationUtils.checkIdpUser(userEntity.nameIdentifier());
    if (!overwritten && entityExists(userEntity.nameIdentifier(), IdpEntityType.IDP_USER)) {
      throw new EntityAlreadyExistsException(
          "Entity %s already exists", userEntity.nameIdentifier().toString());
    }
    IdpUserMetaService.getInstance().insertIdpUser(IdpPOConverters.initializeIdpUserPO(userEntity));
  }

  private static void insertIdpGroup(IdpGroupEntity groupEntity, boolean overwritten)
      throws EntityAlreadyExistsException {
    IdpAuthorizationUtils.checkIdpGroup(groupEntity.nameIdentifier());
    if (!overwritten && entityExists(groupEntity.nameIdentifier(), IdpEntityType.IDP_GROUP)) {
      throw new EntityAlreadyExistsException(
          "Entity %s already exists", groupEntity.nameIdentifier().toString());
    }
    IdpGroupMetaService.getInstance()
        .insertIdpGroup(IdpPOConverters.initializeIdpGroupPO(groupEntity));
  }

  private static boolean entityExists(NameIdentifier ident, IdpEntityType entityType) {
    try {
      switch (entityType) {
        case IDP_USER:
          IdpUserMetaService.getInstance().getIdpUserByUsername(ident.name());
          return true;
        case IDP_GROUP:
          IdpGroupMetaService.getInstance().getIdpGroupByName(ident.name());
          return true;
        default:
          throw new UnsupportedEntityTypeException(
              "Unsupported entity type: %s for exists operation", entityType);
      }
    } catch (NotFoundException e) {
      return false;
    }
  }
}
