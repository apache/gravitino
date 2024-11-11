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

package org.apache.gravitino.storage.relational;

import static org.apache.gravitino.Configs.GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.UnsupportedEntityTypeException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.relational.converters.SQLExceptionConverterFactory;
import org.apache.gravitino.storage.relational.database.H2Database;
import org.apache.gravitino.storage.relational.service.CatalogMetaService;
import org.apache.gravitino.storage.relational.service.FilesetMetaService;
import org.apache.gravitino.storage.relational.service.GroupMetaService;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.apache.gravitino.storage.relational.service.RoleMetaService;
import org.apache.gravitino.storage.relational.service.SchemaMetaService;
import org.apache.gravitino.storage.relational.service.TableColumnMetaService;
import org.apache.gravitino.storage.relational.service.TableMetaService;
import org.apache.gravitino.storage.relational.service.TagMetaService;
import org.apache.gravitino.storage.relational.service.TopicMetaService;
import org.apache.gravitino.storage.relational.service.UserMetaService;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;

/**
 * {@link JDBCBackend} is a jdbc implementation of {@link RelationalBackend} interface. You can use
 * a database that supports the JDBC protocol as storage. If the specified database has special SQL
 * syntax, please implement the SQL statements and methods in MyBatis Mapper separately and switch
 * according to the {@link Configs#ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY} parameter.
 */
public class JDBCBackend implements RelationalBackend {

  private static final Map<JDBCBackendType, String> EMBEDDED_JDBC_DATABASE_MAP =
      ImmutableMap.of(JDBCBackendType.H2, H2Database.class.getCanonicalName());

  // Database instance of this JDBCBackend.
  private JDBCDatabase jdbcDatabase;

  /** Initialize the jdbc backend instance. */
  @Override
  public void initialize(Config config) {
    jdbcDatabase = startJDBCDatabaseIfNecessary(config);

    SqlSessionFactoryHelper.getInstance().init(config);
    SQLExceptionConverterFactory.initConverter(config);
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Entity.EntityType entityType, boolean allFields) throws IOException {
    switch (entityType) {
      case METALAKE:
        return (List<E>) MetalakeMetaService.getInstance().listMetalakes();
      case CATALOG:
        return (List<E>) CatalogMetaService.getInstance().listCatalogsByNamespace(namespace);
      case SCHEMA:
        return (List<E>) SchemaMetaService.getInstance().listSchemasByNamespace(namespace);
      case TABLE:
        return (List<E>) TableMetaService.getInstance().listTablesByNamespace(namespace);
      case FILESET:
        return (List<E>) FilesetMetaService.getInstance().listFilesetsByNamespace(namespace);
      case TOPIC:
        return (List<E>) TopicMetaService.getInstance().listTopicsByNamespace(namespace);
      case TAG:
        return (List<E>) TagMetaService.getInstance().listTagsByNamespace(namespace);
      case USER:
        return (List<E>) UserMetaService.getInstance().listUsersByNamespace(namespace, allFields);
      case ROLE:
        return (List<E>) RoleMetaService.getInstance().listRolesByNamespace(namespace);
      case GROUP:
        return (List<E>) GroupMetaService.getInstance().listGroupsByNamespace(namespace, allFields);
      default:
        throw new UnsupportedEntityTypeException(
            "Unsupported entity type: %s for list operation", entityType);
    }
  }

  @Override
  public boolean exists(NameIdentifier ident, Entity.EntityType entityType) throws IOException {
    try {
      Entity entity = get(ident, entityType);
      return entity != null;
    } catch (NoSuchEntityException ne) {
      return false;
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> void insert(E e, boolean overwritten)
      throws EntityAlreadyExistsException, IOException {
    if (e instanceof BaseMetalake) {
      MetalakeMetaService.getInstance().insertMetalake((BaseMetalake) e, overwritten);
    } else if (e instanceof CatalogEntity) {
      CatalogMetaService.getInstance().insertCatalog((CatalogEntity) e, overwritten);
    } else if (e instanceof SchemaEntity) {
      SchemaMetaService.getInstance().insertSchema((SchemaEntity) e, overwritten);
    } else if (e instanceof TableEntity) {
      TableMetaService.getInstance().insertTable((TableEntity) e, overwritten);
    } else if (e instanceof FilesetEntity) {
      FilesetMetaService.getInstance().insertFileset((FilesetEntity) e, overwritten);
    } else if (e instanceof TopicEntity) {
      TopicMetaService.getInstance().insertTopic((TopicEntity) e, overwritten);
    } else if (e instanceof UserEntity) {
      UserMetaService.getInstance().insertUser((UserEntity) e, overwritten);
    } else if (e instanceof RoleEntity) {
      RoleMetaService.getInstance().insertRole((RoleEntity) e, overwritten);
    } else if (e instanceof GroupEntity) {
      GroupMetaService.getInstance().insertGroup((GroupEntity) e, overwritten);
    } else if (e instanceof TagEntity) {
      TagMetaService.getInstance().insertTag((TagEntity) e, overwritten);
    } else {
      throw new UnsupportedEntityTypeException(
          "Unsupported entity type: %s for insert operation", e.getClass());
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Entity.EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException {
    switch (entityType) {
      case METALAKE:
        return (E) MetalakeMetaService.getInstance().updateMetalake(ident, updater);
      case CATALOG:
        return (E) CatalogMetaService.getInstance().updateCatalog(ident, updater);
      case SCHEMA:
        return (E) SchemaMetaService.getInstance().updateSchema(ident, updater);
      case TABLE:
        return (E) TableMetaService.getInstance().updateTable(ident, updater);
      case FILESET:
        return (E) FilesetMetaService.getInstance().updateFileset(ident, updater);
      case TOPIC:
        return (E) TopicMetaService.getInstance().updateTopic(ident, updater);
      case USER:
        return (E) UserMetaService.getInstance().updateUser(ident, updater);
      case GROUP:
        return (E) GroupMetaService.getInstance().updateGroup(ident, updater);
      case ROLE:
        return (E) RoleMetaService.getInstance().updateRole(ident, updater);
      case TAG:
        return (E) TagMetaService.getInstance().updateTag(ident, updater);
      default:
        throw new UnsupportedEntityTypeException(
            "Unsupported entity type: %s for update operation", entityType);
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(
      NameIdentifier ident, Entity.EntityType entityType)
      throws NoSuchEntityException, IOException {
    switch (entityType) {
      case METALAKE:
        return (E) MetalakeMetaService.getInstance().getMetalakeByIdentifier(ident);
      case CATALOG:
        return (E) CatalogMetaService.getInstance().getCatalogByIdentifier(ident);
      case SCHEMA:
        return (E) SchemaMetaService.getInstance().getSchemaByIdentifier(ident);
      case TABLE:
        return (E) TableMetaService.getInstance().getTableByIdentifier(ident);
      case FILESET:
        return (E) FilesetMetaService.getInstance().getFilesetByIdentifier(ident);
      case TOPIC:
        return (E) TopicMetaService.getInstance().getTopicByIdentifier(ident);
      case USER:
        return (E) UserMetaService.getInstance().getUserByIdentifier(ident);
      case GROUP:
        return (E) GroupMetaService.getInstance().getGroupByIdentifier(ident);
      case ROLE:
        return (E) RoleMetaService.getInstance().getRoleByIdentifier(ident);
      case TAG:
        return (E) TagMetaService.getInstance().getTagByIdentifier(ident);
      default:
        throw new UnsupportedEntityTypeException(
            "Unsupported entity type: %s for get operation", entityType);
    }
  }

  @Override
  public boolean delete(NameIdentifier ident, Entity.EntityType entityType, boolean cascade)
      throws IOException {
    switch (entityType) {
      case METALAKE:
        return MetalakeMetaService.getInstance().deleteMetalake(ident, cascade);
      case CATALOG:
        return CatalogMetaService.getInstance().deleteCatalog(ident, cascade);
      case SCHEMA:
        return SchemaMetaService.getInstance().deleteSchema(ident, cascade);
      case TABLE:
        return TableMetaService.getInstance().deleteTable(ident);
      case FILESET:
        return FilesetMetaService.getInstance().deleteFileset(ident);
      case TOPIC:
        return TopicMetaService.getInstance().deleteTopic(ident);
      case USER:
        return UserMetaService.getInstance().deleteUser(ident);
      case GROUP:
        return GroupMetaService.getInstance().deleteGroup(ident);
      case ROLE:
        return RoleMetaService.getInstance().deleteRole(ident);
      case TAG:
        return TagMetaService.getInstance().deleteTag(ident);
      default:
        throw new UnsupportedEntityTypeException(
            "Unsupported entity type: %s for delete operation", entityType);
    }
  }

  @Override
  public int hardDeleteLegacyData(Entity.EntityType entityType, long legacyTimeline)
      throws IOException {
    switch (entityType) {
      case METALAKE:
        return MetalakeMetaService.getInstance()
            .deleteMetalakeMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case CATALOG:
        return CatalogMetaService.getInstance()
            .deleteCatalogMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case SCHEMA:
        return SchemaMetaService.getInstance()
            .deleteSchemaMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case TABLE:
        return TableMetaService.getInstance()
            .deleteTableMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case FILESET:
        return FilesetMetaService.getInstance()
            .deleteFilesetAndVersionMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case TOPIC:
        return TopicMetaService.getInstance()
            .deleteTopicMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case USER:
        return UserMetaService.getInstance()
            .deleteUserMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case GROUP:
        return GroupMetaService.getInstance()
            .deleteGroupMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case ROLE:
        return RoleMetaService.getInstance()
            .deleteRoleMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case TAG:
        return TagMetaService.getInstance()
            .deleteTagMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case COLUMN:
        return TableColumnMetaService.getInstance()
            .deleteColumnsByLegacyTimeline(legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case AUDIT:
        return 0;
        // TODO: Implement hard delete logic for these entity types.

      default:
        throw new IllegalArgumentException(
            "Unsupported entity type when collectAndRemoveLegacyData: " + entityType);
    }
  }

  @Override
  public int deleteOldVersionData(Entity.EntityType entityType, long versionRetentionCount)
      throws IOException {
    switch (entityType) {
      case METALAKE:
      case CATALOG:
      case SCHEMA:
      case TABLE:
      case COLUMN:
      case TOPIC:
      case USER:
      case GROUP:
      case AUDIT:
      case ROLE:
      case TAG:
        // These entity types have not implemented multi-versions, so we can skip.
        return 0;

      case FILESET:
        return FilesetMetaService.getInstance()
            .deleteFilesetVersionsByRetentionCount(
                versionRetentionCount, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);

      default:
        throw new IllegalArgumentException(
            "Unsupported entity type when collectAndRemoveOldVersionData: " + entityType);
    }
  }

  @Override
  public void close() throws IOException {
    SqlSessionFactoryHelper.getInstance().close();

    if (jdbcDatabase != null) {
      jdbcDatabase.close();
    }
  }

  @Override
  public List<MetadataObject> listAssociatedMetadataObjectsForTag(NameIdentifier tagIdent)
      throws IOException {
    return TagMetaService.getInstance().listAssociatedMetadataObjectsForTag(tagIdent);
  }

  @Override
  public List<TagEntity> listAssociatedTagsForMetadataObject(
      NameIdentifier objectIdent, Entity.EntityType objectType)
      throws NoSuchEntityException, IOException {
    return TagMetaService.getInstance().listTagsForMetadataObject(objectIdent, objectType);
  }

  @Override
  public TagEntity getTagForMetadataObject(
      NameIdentifier objectIdent, Entity.EntityType objectType, NameIdentifier tagIdent)
      throws NoSuchEntityException, IOException {
    return TagMetaService.getInstance().getTagForMetadataObject(objectIdent, objectType, tagIdent);
  }

  @Override
  public List<TagEntity> associateTagsWithMetadataObject(
      NameIdentifier objectIdent,
      Entity.EntityType objectType,
      NameIdentifier[] tagsToAdd,
      NameIdentifier[] tagsToRemove)
      throws NoSuchEntityException, EntityAlreadyExistsException, IOException {
    return TagMetaService.getInstance()
        .associateTagsWithMetadataObject(objectIdent, objectType, tagsToAdd, tagsToRemove);
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> listEntitiesByRelation(
      Type relType, NameIdentifier nameIdentifier, Entity.EntityType identType, boolean allFields) {
    switch (relType) {
      case OWNER_REL:
        List<E> list = Lists.newArrayList();
        OwnerMetaService.getInstance()
            .getOwner(nameIdentifier, identType)
            .ifPresent(e -> list.add((E) e));
        return list;
      case METADATA_OBJECT_ROLE_REL:
        return (List<E>)
            RoleMetaService.getInstance()
                .listRolesByMetadataObject(nameIdentifier, identType, allFields);
      case ROLE_GROUP_REL:
        if (identType == Entity.EntityType.ROLE) {
          return (List<E>) GroupMetaService.getInstance().listGroupsByRoleIdent(nameIdentifier);
        } else {
          throw new IllegalArgumentException(
              String.format("ROLE_GROUP_REL doesn't support type %s", identType.name()));
        }
      case ROLE_USER_REL:
        if (identType == Entity.EntityType.ROLE) {
          return (List<E>) UserMetaService.getInstance().listUsersByRoleIdent(nameIdentifier);
        } else {
          throw new IllegalArgumentException(
              String.format("ROLE_USER_REL doesn't support type %s", identType.name()));
        }
      default:
        throw new IllegalArgumentException(
            String.format("Doesn't support the relation type %s", relType));
    }
  }

  @Override
  public void insertRelation(
      SupportsRelationOperations.Type relType,
      NameIdentifier srcIdentifier,
      Entity.EntityType srcType,
      NameIdentifier dstIdentifier,
      Entity.EntityType dstType,
      boolean override) {
    switch (relType) {
      case OWNER_REL:
        OwnerMetaService.getInstance().setOwner(srcIdentifier, srcType, dstIdentifier, dstType);
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Doesn't support the relation type %s", relType));
    }
  }

  public enum JDBCBackendType {
    H2(true),
    MYSQL(false),
    POSTGRESQL(false);

    private final boolean embedded;

    JDBCBackendType(boolean embedded) {
      this.embedded = embedded;
    }

    public static JDBCBackendType fromURI(String jdbcURI) {
      if (jdbcURI.startsWith("jdbc:h2")) {
        return JDBCBackendType.H2;
      } else if (jdbcURI.startsWith("jdbc:mysql")) {
        return JDBCBackendType.MYSQL;
      } else if (jdbcURI.startsWith("jdbc:postgresql")) {
        return JDBCBackendType.POSTGRESQL;
      } else {
        throw new IllegalArgumentException("Unknown JDBC URI: " + jdbcURI);
      }
    }

    public static JDBCBackendType fromString(String jdbcType) {
      switch (jdbcType) {
        case "h2":
          return JDBCBackendType.H2;
        case "mysql":
          return JDBCBackendType.MYSQL;
        case "postgresql":
          return JDBCBackendType.POSTGRESQL;
        default:
          throw new IllegalArgumentException("Unknown JDBC type: " + jdbcType);
      }
    }
  }

  /** Start JDBC database if necessary. For example, start the H2 database if the backend is H2. */
  private static JDBCDatabase startJDBCDatabaseIfNecessary(Config config) {
    String jdbcUrl = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL);
    JDBCBackendType jdbcBackendType = JDBCBackendType.fromURI(jdbcUrl);

    // Not an embedded database.
    if (!jdbcBackendType.embedded) {
      return null;
    }

    try {
      JDBCDatabase jdbcDatabase =
          (JDBCDatabase)
              Class.forName(EMBEDDED_JDBC_DATABASE_MAP.get(jdbcBackendType))
                  .getDeclaredConstructor()
                  .newInstance();
      jdbcDatabase.initialize(config);
      return jdbcDatabase;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create and initialize JDBCBackend.", e);
    }
  }
}
