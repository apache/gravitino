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
package org.apache.gravitino.authorization.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link KafkaAuthorizationStateProvider} backed by Gravitino's {@link EntityStore}.
 *
 * <p>Temporarily switches the thread context classloader to {@code
 * GravitinoEnv.class.getClassLoader()} on each call so isolated authorization plugin classloaders
 * do not break MyBatis/EntityStore mapper resolution.
 */
public class EntityStoreAuthorizationStateProvider implements KafkaAuthorizationStateProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(EntityStoreAuthorizationStateProvider.class);

  /** Creates a new {@link EntityStoreAuthorizationStateProvider}. */
  public EntityStoreAuthorizationStateProvider() {}

  @Override
  public Optional<String> resolveCatalogName(String metalake, @Nullable Long catalogId) {
    if (catalogId == null || StringUtils.isBlank(metalake)) {
      return Optional.empty();
    }
    return withBaseClassLoader(
        () -> {
          try {
            EntityStore store = entityStore();
            List<CatalogEntity> catalogs =
                store.list(
                    NamespaceUtil.ofCatalog(metalake),
                    CatalogEntity.class,
                    Entity.EntityType.CATALOG);
            for (CatalogEntity catalog : catalogs) {
              if (catalogId.equals(catalog.id())) {
                return Optional.of(catalog.name());
              }
            }
            return Optional.empty();
          } catch (NoSuchEntityException e) {
            return Optional.empty();
          } catch (IOException e) {
            throw new AuthorizationPluginException(
                e,
                "Failed to resolve catalog name for catalog ID %d in metalake %s",
                catalogId,
                metalake);
          }
        });
  }

  @Override
  public List<String> listUserNamesByRole(String metalake, String roleName) {
    return withBaseClassLoader(
        () -> {
          try {
            EntityStore store = entityStore();
            List<? extends Entity> entities =
                store
                    .relationOperations()
                    .listEntitiesByRelation(
                        SupportsRelationOperations.Type.ROLE_USER_REL,
                        AuthorizationUtils.ofRole(metalake, roleName),
                        Entity.EntityType.ROLE);
            List<String> users = new ArrayList<>();
            for (Entity entity : entities) {
              if (entity instanceof UserEntity) {
                users.add(((UserEntity) entity).name());
              }
            }
            return Collections.unmodifiableList(users);
          } catch (NoSuchEntityException e) {
            return Collections.emptyList();
          } catch (IOException e) {
            throw new AuthorizationPluginException(
                e, "Failed to list users bound to role %s in metalake %s", roleName, metalake);
          }
        });
  }

  @Override
  public List<String> listGroupNamesByRole(String metalake, String roleName) {
    return withBaseClassLoader(
        () -> {
          try {
            EntityStore store = entityStore();
            List<? extends Entity> entities =
                store
                    .relationOperations()
                    .listEntitiesByRelation(
                        SupportsRelationOperations.Type.ROLE_GROUP_REL,
                        AuthorizationUtils.ofRole(metalake, roleName),
                        Entity.EntityType.ROLE);
            List<String> groups = new ArrayList<>();
            for (Entity entity : entities) {
              if (entity instanceof GroupEntity) {
                groups.add(((GroupEntity) entity).name());
              }
            }
            return Collections.unmodifiableList(groups);
          } catch (NoSuchEntityException e) {
            return Collections.emptyList();
          } catch (IOException e) {
            throw new AuthorizationPluginException(
                e, "Failed to list groups bound to role %s in metalake %s", roleName, metalake);
          }
        });
  }

  @Override
  public List<Role> listRolesForUser(String metalake, String username) {
    return withBaseClassLoader(
        () -> {
          try {
            EntityStore store = entityStore();
            UserEntity user =
                store.get(
                    AuthorizationUtils.ofUser(metalake, username),
                    Entity.EntityType.USER,
                    UserEntity.class);
            return loadRolesByNames(store, metalake, user.roleNames());
          } catch (NoSuchEntityException e) {
            return Collections.emptyList();
          } catch (IOException e) {
            throw new AuthorizationPluginException(
                e, "Failed to load roles for user %s in metalake %s", username, metalake);
          }
        });
  }

  @Override
  public List<Role> listRolesForGroup(String metalake, String groupName) {
    return withBaseClassLoader(
        () -> {
          try {
            EntityStore store = entityStore();
            GroupEntity group =
                store.get(
                    AuthorizationUtils.ofGroup(metalake, groupName),
                    Entity.EntityType.GROUP,
                    GroupEntity.class);
            return loadRolesByNames(store, metalake, group.roleNames());
          } catch (NoSuchEntityException e) {
            return Collections.emptyList();
          } catch (IOException e) {
            throw new AuthorizationPluginException(
                e, "Failed to load roles for group %s in metalake %s", groupName, metalake);
          }
        });
  }

  @Override
  public List<MetadataObject> listOwnedObjectsForPrincipal(
      String metalake, @Nullable String catalogName, Owner.Type ownerType, String ownerName) {
    if (StringUtils.isBlank(metalake) || ownerType == null || StringUtils.isBlank(ownerName)) {
      return Collections.emptyList();
    }

    return withBaseClassLoader(
        () -> {
          EntityStore store = entityStore();
          List<MetadataObject> owned = new ArrayList<>();

          checkAndAddOwner(
              store,
              NameIdentifier.of(metalake),
              Entity.EntityType.METALAKE,
              MetadataObjects.of(Collections.singletonList(metalake), MetadataObject.Type.METALAKE),
              ownerType,
              ownerName,
              owned);

          if (StringUtils.isBlank(catalogName)) {
            return Collections.unmodifiableList(owned);
          }

          checkAndAddOwner(
              store,
              NameIdentifierUtil.ofCatalog(metalake, catalogName),
              Entity.EntityType.CATALOG,
              MetadataObjects.of(
                  Collections.singletonList(catalogName), MetadataObject.Type.CATALOG),
              ownerType,
              ownerName,
              owned);

          List<SchemaEntity> schemas;
          try {
            schemas =
                store.list(
                    NamespaceUtil.ofSchema(metalake, catalogName),
                    SchemaEntity.class,
                    Entity.EntityType.SCHEMA);
          } catch (NoSuchEntityException e) {
            return Collections.unmodifiableList(owned);
          } catch (IOException e) {
            throw new AuthorizationPluginException(
                e,
                "Failed to list schemas for ownership lookup under %s.%s",
                metalake,
                catalogName);
          }

          for (SchemaEntity schema : schemas) {
            NameIdentifier schemaIdent =
                NameIdentifierUtil.ofSchema(metalake, catalogName, schema.name());
            checkAndAddOwner(
                store,
                schemaIdent,
                Entity.EntityType.SCHEMA,
                MetadataObjects.of(catalogName, schema.name(), MetadataObject.Type.SCHEMA),
                ownerType,
                ownerName,
                owned);

            List<TopicEntity> topics;
            try {
              topics =
                  store.list(
                      NamespaceUtil.ofTopic(metalake, catalogName, schema.name()),
                      TopicEntity.class,
                      Entity.EntityType.TOPIC);
            } catch (NoSuchEntityException e) {
              continue;
            } catch (IOException e) {
              throw new AuthorizationPluginException(
                  e,
                  "Failed to list topics for ownership lookup under %s.%s.%s",
                  metalake,
                  catalogName,
                  schema.name());
            }

            addOwnedTopicsInSchema(
                store, metalake, catalogName, schema.name(), topics, ownerType, ownerName, owned);
          }

          return Collections.unmodifiableList(owned);
        });
  }

  private static void addOwnedTopicsInSchema(
      EntityStore store,
      String metalake,
      String catalogName,
      String schemaName,
      List<TopicEntity> topics,
      Owner.Type expectedOwnerType,
      String expectedOwnerName,
      List<MetadataObject> accumulator) {
    if (topics == null || topics.isEmpty()) {
      return;
    }

    List<NameIdentifier> topicIdents = new ArrayList<>(topics.size());
    for (TopicEntity topic : topics) {
      topicIdents.add(NameIdentifierUtil.ofTopic(metalake, catalogName, schemaName, topic.name()));
    }

    try {
      List<RelationalEntity<?>> ownerRelations =
          store
              .relationOperations()
              .batchListEntitiesByRelation(
                  SupportsRelationOperations.Type.OWNER_REL, topicIdents, Entity.EntityType.TOPIC);
      if (ownerRelations != null) {
        for (RelationalEntity<?> relation : ownerRelations) {
          if (relation != null
              && relation.source() != null
              && matchesOwner(relation.targetEntity(), expectedOwnerType, expectedOwnerName)) {
            accumulator.add(
                NameIdentifierUtil.toMetadataObjectForRead(
                    relation.source(), Entity.EntityType.TOPIC));
          }
        }
        return;
      }
    } catch (UnsupportedOperationException e) {
      LOG.debug(
          "Batch owner lookup is unsupported by current EntityStore; falling back to per-topic queries",
          e);
    } catch (NoSuchEntityException e) {
      LOG.debug(
          "A topic under {}.{}.{} disappeared during batch owner lookup; falling back to per-topic queries",
          metalake,
          catalogName,
          schemaName,
          e);
    } catch (IOException e) {
      throw new AuthorizationPluginException(
          e,
          "Failed to batch-query topic owners under %s.%s.%s",
          metalake,
          catalogName,
          schemaName);
    }

    for (NameIdentifier topicIdent : topicIdents) {
      MetadataObject topicObject =
          NameIdentifierUtil.toMetadataObjectForRead(topicIdent, Entity.EntityType.TOPIC);
      checkAndAddOwner(
          store,
          topicIdent,
          Entity.EntityType.TOPIC,
          topicObject,
          expectedOwnerType,
          expectedOwnerName,
          accumulator);
    }
  }

  private static List<Role> loadRolesByNames(
      EntityStore store, String metalake, List<String> roleNames) throws IOException {
    if (roleNames == null || roleNames.isEmpty()) {
      return Collections.emptyList();
    }
    List<Role> roles = new ArrayList<>(roleNames.size());
    for (String roleName : roleNames) {
      try {
        RoleEntity roleEntity =
            store.get(
                AuthorizationUtils.ofRole(metalake, roleName),
                Entity.EntityType.ROLE,
                RoleEntity.class);
        roles.add(roleEntity);
      } catch (NoSuchEntityException e) {
        LOG.debug("Skipping missing role {} while resolving principal roles", roleName);
      }
    }
    return Collections.unmodifiableList(roles);
  }

  private static void checkAndAddOwner(
      EntityStore store,
      NameIdentifier ident,
      Entity.EntityType entityType,
      MetadataObject metadataObject,
      Owner.Type expectedOwnerType,
      String expectedOwnerName,
      List<MetadataObject> accumulator) {
    try {
      List<? extends Entity> owners =
          store
              .relationOperations()
              .listEntitiesByRelation(SupportsRelationOperations.Type.OWNER_REL, ident, entityType);
      for (Entity entity : owners) {
        if (matchesOwner(entity, expectedOwnerType, expectedOwnerName)) {
          accumulator.add(metadataObject);
          return;
        }
      }
    } catch (NoSuchEntityException e) {
      LOG.debug("Skipping non-existent metadata object {} during owner lookup", ident);
    } catch (IOException e) {
      throw new AuthorizationPluginException(
          e, "Failed to query owner relation for metadata object %s", ident);
    }
  }

  private static boolean matchesOwner(
      Entity entity, Owner.Type expectedOwnerType, String expectedOwnerName) {
    if (expectedOwnerType == Owner.Type.USER && entity instanceof UserEntity) {
      return expectedOwnerName.equals(((UserEntity) entity).name());
    }
    if (expectedOwnerType == Owner.Type.GROUP && entity instanceof GroupEntity) {
      return expectedOwnerName.equals(((GroupEntity) entity).name());
    }
    return false;
  }

  private static EntityStore entityStore() {
    return GravitinoEnv.getInstance().entityStore();
  }

  private static <T> T withBaseClassLoader(Supplier<T> action) {
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(GravitinoEnv.class.getClassLoader());
    try {
      return action.get();
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }
}
