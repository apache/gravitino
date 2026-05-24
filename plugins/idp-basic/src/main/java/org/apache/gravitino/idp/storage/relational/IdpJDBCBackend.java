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

import com.google.common.collect.ImmutableMap;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
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
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.JDBCDatabase;
import org.apache.gravitino.storage.relational.converters.SQLExceptionConverterFactory;
import org.apache.gravitino.storage.relational.database.H2Database;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;

/** JDBC backend for built-in IdP entities. */
public class IdpJDBCBackend implements Closeable {

  private static final Map<JDBCBackendType, String> EMBEDDED_JDBC_DATABASE_MAP =
      ImmutableMap.of(JDBCBackendType.H2, H2Database.class.getCanonicalName());

  private JDBCDatabase jdbcDatabase;

  /** Initializes the JDBC backend and MyBatis session factory. */
  public void initialize(Config config) {
    jdbcDatabase = startEmbeddedDatabaseIfNecessary(config);
    SqlSessionFactoryHelper.getInstance().init(config);
    SQLExceptionConverterFactory.initConverter(config);
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

  public void changePassword(String username, String passwordHash) throws NotFoundException {
    if (!IdpUserMetaService.getInstance().updateIdpUserPassword(username, passwordHash)) {
      throw new NotFoundException("IdP user %s does not exist", username);
    }
  }

  public void addUsersToGroup(String groupName, List<String> usernames) {
    IdpGroupMetaService.getInstance().addUsersToGroup(groupName, usernames);
  }

  public void removeUsersFromGroup(String groupName, List<String> usernames) {
    IdpGroupMetaService.getInstance().removeUsersFromGroup(groupName, usernames);
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

  @Override
  public void close() throws IOException {
    SqlSessionFactoryHelper.getInstance().close();
    SQLExceptionConverterFactory.close();
    if (jdbcDatabase != null) {
      jdbcDatabase.close();
    }
  }

  private static JDBCDatabase startEmbeddedDatabaseIfNecessary(Config config) {
    String jdbcUrl = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL);
    JDBCBackendType jdbcBackendType = JDBCBackendType.fromURI(jdbcUrl);
    if (jdbcBackendType != JDBCBackendType.H2) {
      return null;
    }

    try {
      JDBCDatabase database =
          (JDBCDatabase)
              Class.forName(EMBEDDED_JDBC_DATABASE_MAP.get(jdbcBackendType))
                  .getDeclaredConstructor()
                  .newInstance();
      database.initialize(config);
      return database;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create and initialize IdP JDBC backend.", e);
    }
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
      List<String> usernames =
          IdpGroupMetaService.getInstance().listUsernamesByGroupName(groupName);
      return IdpPOConverters.fromIdpGroupPO(groupPO, usernames);
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
