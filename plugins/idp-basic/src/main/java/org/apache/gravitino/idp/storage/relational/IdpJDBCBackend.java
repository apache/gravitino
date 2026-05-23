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
import java.util.function.Function;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.UnsupportedEntityTypeException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.idp.authorization.IdpAuthorizationUtils;
import org.apache.gravitino.idp.exception.NotFoundException;
import org.apache.gravitino.idp.meta.IdpGroupEntity;
import org.apache.gravitino.idp.meta.IdpUserEntity;
import org.apache.gravitino.idp.storage.converter.IdpPOConverters;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.gravitino.idp.storage.service.IdpGroupMetaService;
import org.apache.gravitino.idp.storage.service.IdpUserMetaService;
import org.apache.gravitino.storage.relational.JDBCBackend;

/**
 * JDBC backend for built-in IdP entities. It mirrors {@link JDBCBackend} but only supports {@link
 * Entity.EntityType#IDP_USER} and {@link Entity.EntityType#IDP_GROUP}.
 *
 * <p>Entity/PO conversion stays in this class so {@link IdpUserMetaService} and {@link
 * IdpGroupMetaService} remain unchanged PO-level services.
 */
public class IdpJDBCBackend extends JDBCBackend {

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Entity.EntityType entityType, boolean allFields) throws IOException {
    throw new UnsupportedOperationException(
        "List is not supported for built-in IdP entity type: " + entityType);
  }

  @Override
  public <E extends Entity & HasIdentifier> void insert(E e, boolean overwritten)
      throws EntityAlreadyExistsException, IOException {
    if (e instanceof IdpUserEntity) {
      insertIdpUser((IdpUserEntity) e, overwritten);
    } else if (e instanceof IdpGroupEntity) {
      insertIdpGroup((IdpGroupEntity) e, overwritten);
    } else {
      throw new UnsupportedEntityTypeException(
          "Unsupported entity type: %s for insert operation", e.type());
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Entity.EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException {
    throw new UnsupportedOperationException(
        "Update is not supported for built-in IdP entity type: " + entityType);
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(
      NameIdentifier ident, Entity.EntityType entityType) throws IOException {
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

  @Override
  public <E extends Entity & HasIdentifier> List<E> batchGet(
      List<NameIdentifier> identifiers, Entity.EntityType entityType) {
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

  @Override
  public boolean delete(NameIdentifier ident, Entity.EntityType entityType, boolean cascade)
      throws IOException {
    if (!exists(ident, entityType)) {
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

  @Override
  public int hardDeleteLegacyData(Entity.EntityType entityType, long legacyTimeline)
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

  @Override
  public int deleteOldVersionData(Entity.EntityType entityType, long versionRetentionCount)
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

  @Override
  public int batchDelete(
      List<Pair<NameIdentifier, Entity.EntityType>> entitiesToDelete, boolean cascade)
      throws IOException {
    throw new IllegalArgumentException("Batch delete is not supported for built-in IdP entities");
  }

  @Override
  public <E extends Entity & HasIdentifier> void batchPut(List<E> entities, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    throw new IllegalArgumentException("Batch put is not supported for built-in IdP entities");
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> listEntitiesByRelation(
      SupportsRelationOperations.Type relType,
      NameIdentifier nameIdentifier,
      Entity.EntityType identType,
      boolean allFields)
      throws IOException {
    throw new UnsupportedOperationException(
        "Relation operations are not supported for built-in IdP entities");
  }

  @Override
  public List<RelationalEntity<?>> batchListEntitiesByRelation(
      SupportsRelationOperations.Type relType,
      List<NameIdentifier> nameIdentifiers,
      Entity.EntityType identType)
      throws IOException {
    throw new UnsupportedOperationException(
        "Relation operations are not supported for built-in IdP entities");
  }

  @Override
  public <E extends Entity & HasIdentifier> E getEntityByRelation(
      SupportsRelationOperations.Type relType,
      NameIdentifier srcIdentifier,
      Entity.EntityType srcType,
      NameIdentifier destEntityIdent)
      throws IOException, NoSuchEntityException {
    throw new UnsupportedOperationException(
        "Relation operations are not supported for built-in IdP entities");
  }

  @Override
  public void insertRelation(
      SupportsRelationOperations.Type relType,
      NameIdentifier srcIdentifier,
      Entity.EntityType srcType,
      NameIdentifier dstIdentifier,
      Entity.EntityType dstType,
      boolean override) {
    throw new UnsupportedOperationException(
        "Relation operations are not supported for built-in IdP entities");
  }

  @Override
  public void batchInsertRelations(
      SupportsRelationOperations.Type relType,
      List<NameIdentifier> srcIdentifiers,
      Entity.EntityType srcType,
      NameIdentifier dstIdentifier,
      Entity.EntityType dstType,
      boolean override) {
    throw new UnsupportedOperationException(
        "Relation operations are not supported for built-in IdP entities");
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> updateEntityRelations(
      SupportsRelationOperations.Type relType,
      NameIdentifier srcEntityIdent,
      Entity.EntityType srcEntityType,
      NameIdentifier[] destEntitiesToAdd,
      NameIdentifier[] destEntitiesToRemove)
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException {
    throw new UnsupportedOperationException(
        "Relation operations are not supported for built-in IdP entities");
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
          Entity.EntityType.IDP_USER.name().toLowerCase(),
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
          Entity.EntityType.IDP_GROUP.name().toLowerCase(),
          ident.name());
    }
  }

  private static void insertIdpUser(IdpUserEntity userEntity, boolean overwritten)
      throws EntityAlreadyExistsException {
    IdpAuthorizationUtils.checkIdpUser(userEntity.nameIdentifier());
    if (!overwritten && entityExists(userEntity.nameIdentifier(), Entity.EntityType.IDP_USER)) {
      throw new EntityAlreadyExistsException(
          "Entity %s already exists", userEntity.nameIdentifier().toString());
    }
    IdpUserMetaService.getInstance().insertIdpUser(IdpPOConverters.initializeIdpUserPO(userEntity));
  }

  private static void insertIdpGroup(IdpGroupEntity groupEntity, boolean overwritten)
      throws EntityAlreadyExistsException {
    IdpAuthorizationUtils.checkIdpGroup(groupEntity.nameIdentifier());
    if (!overwritten && entityExists(groupEntity.nameIdentifier(), Entity.EntityType.IDP_GROUP)) {
      throw new EntityAlreadyExistsException(
          "Entity %s already exists", groupEntity.nameIdentifier().toString());
    }
    IdpGroupMetaService.getInstance()
        .insertIdpGroup(IdpPOConverters.initializeIdpGroupPO(groupEntity));
  }

  private static boolean entityExists(NameIdentifier ident, Entity.EntityType entityType) {
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
