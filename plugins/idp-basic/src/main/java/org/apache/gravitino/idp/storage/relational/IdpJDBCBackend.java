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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.Config;
import org.apache.gravitino.idp.exception.AlreadyExistsException;
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

/** JDBC backend for built-in IdP entities. */
public class IdpJDBCBackend implements Closeable {

  private final IdpJdbcSessionSupport sessionSupport = new IdpJdbcSessionSupport();

  public void initialize(Config config) {
    sessionSupport.initialize(config);
  }

  public boolean exists(String name, IdpEntityType entityType) throws IOException {
    return entityExists(name, entityType);
  }

  public <E extends IdpEntity> void insert(E entity, boolean overwritten)
      throws AlreadyExistsException, IOException {
    if (entity instanceof IdpUserEntity) {
      insertIdpUser((IdpUserEntity) entity, overwritten);
    } else if (entity instanceof IdpGroupEntity) {
      insertIdpGroup((IdpGroupEntity) entity, overwritten);
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported entity type: %s for insert operation", entity.type()));
    }
  }

  public <E extends IdpEntity> E get(String name, IdpEntityType entityType) throws IOException {
    switch (entityType) {
      case IDP_USER:
        return (E) getIdpUser(name);
      case IDP_GROUP:
        return (E) getIdpGroup(name);
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported entity type: %s for get operation", entityType));
    }
  }

  public <E extends IdpEntity> List<E> batchGet(List<String> names, IdpEntityType entityType) {
    List<E> entities = new ArrayList<>(names.size());
    for (String name : names) {
      try {
        entities.add(get(name, entityType));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return entities;
  }

  public boolean delete(String name, IdpEntityType entityType, boolean cascade) throws IOException {
    if (!entityExists(name, entityType)) {
      return false;
    }
    switch (entityType) {
      case IDP_USER:
        return IdpUserMetaService.getInstance().deleteIdpUser(name);
      case IDP_GROUP:
        return IdpGroupMetaService.getInstance().deleteIdpGroup(name);
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported entity type: %s for delete operation", entityType));
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

  @Override
  public void close() throws IOException {
    sessionSupport.close();
  }

  private static IdpUserEntity getIdpUser(String username) {
    try {
      IdpUserPO userPO = IdpUserMetaService.getInstance().getIdpUserByUsername(username);
      List<String> groupNames = IdpUserMetaService.getInstance().listGroupNamesByUsername(username);
      return IdpPOConverters.fromIdpUserPO(userPO, groupNames);
    } catch (NotFoundException e) {
      throw new NotFoundException("IdP user %s does not exist", username);
    }
  }

  private static IdpGroupEntity getIdpGroup(String groupName) {
    try {
      IdpGroupPO groupPO = IdpGroupMetaService.getInstance().getIdpGroupByName(groupName);
      List<String> userNames =
          IdpGroupMetaService.getInstance().listUsernamesByGroupName(groupName);
      return IdpPOConverters.fromIdpGroupPO(groupPO, userNames);
    } catch (NotFoundException e) {
      throw new NotFoundException("IdP group %s does not exist", groupName);
    }
  }

  private static void insertIdpUser(IdpUserEntity userEntity, boolean overwritten)
      throws AlreadyExistsException {
    if (!overwritten && entityExists(userEntity.name(), IdpEntityType.IDP_USER)) {
      throw new AlreadyExistsException("IdP user %s already exists", userEntity.name());
    }
    IdpUserMetaService.getInstance().insertIdpUser(IdpPOConverters.initializeIdpUserPO(userEntity));
  }

  private static void insertIdpGroup(IdpGroupEntity groupEntity, boolean overwritten)
      throws AlreadyExistsException {
    if (!overwritten && entityExists(groupEntity.name(), IdpEntityType.IDP_GROUP)) {
      throw new AlreadyExistsException("IdP group %s already exists", groupEntity.name());
    }
    IdpGroupMetaService.getInstance()
        .insertIdpGroup(IdpPOConverters.initializeIdpGroupPO(groupEntity));
  }

  private static boolean entityExists(String name, IdpEntityType entityType) {
    try {
      switch (entityType) {
        case IDP_USER:
          IdpUserMetaService.getInstance().getIdpUserByUsername(name);
          return true;
        case IDP_GROUP:
          IdpGroupMetaService.getInstance().getIdpGroupByName(name);
          return true;
        default:
          throw new IllegalArgumentException(
              String.format("Unsupported entity type: %s for exists operation", entityType));
      }
    } catch (NotFoundException e) {
      return false;
    }
  }
}
