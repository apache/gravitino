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
package org.apache.gravitino.cache;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.utils.NamespaceUtil;

/**
 * Reverse index rules for different entity types. This class defines how to process reverse
 * indexing for UserEntity, GroupEntity, and RoleEntity. <br>
 * For example: <br>
 * - UserEntity role is {metalake-name}.system.user.{user-name}:USER-{serial-number} <br>
 * - GroupEntity role is {metalake-name}.system.group.{group-name}:GROUP-{serial-number} <br>
 * - RoleEntity role is {metalake-name}.system.role.{role-name}:ROLE-{serial-number} <br>
 */
public class ReverseIndexRules {

  /** UserEntity reverse index processor */
  public static final ReverseIndexCache.ReverseIndexRule USER_ROLE_REVERSE_RULE =
      (entity, key, reverseIndexCache) -> {
        UserEntity userEntity = (UserEntity) entity;
        if (userEntity.roleNames() != null) {
          userEntity
              .roleNames()
              .forEach(
                  role -> {
                    Namespace ns = NamespaceUtil.ofRole(userEntity.namespace().level(0));
                    NameIdentifier nameIdentifier = NameIdentifier.of(ns, role);
                    reverseIndexCache.put(nameIdentifier, Entity.EntityType.ROLE, key);
                  });
        }
      };

  public static final ReverseIndexCache.ReverseIndexRule USER_OWNERSHIP_REVERSE_RULE =
      (entity, key, reverseIndexCache) -> {
        UserEntity userEntity = (UserEntity) entity;
        // Handle Securable Objects -> User reverse index, so the key type is User and the value
        // type is securable Object.
        if (key.relationType() == SupportsRelationOperations.Type.OWNER_REL) {
          reverseIndexCache.put(userEntity.nameIdentifier(), EntityType.USER, key);
        }
      };

  /** GroupEntity reverse index processor */
  public static final ReverseIndexCache.ReverseIndexRule GROUP_ROLE_REVERSE_RULE =
      (entity, key, reverseIndexCache) -> {
        GroupEntity groupEntity = (GroupEntity) entity;
        if (groupEntity.roleNames() != null) {
          groupEntity
              .roleNames()
              .forEach(
                  role -> {
                    Namespace ns = NamespaceUtil.ofRole(groupEntity.namespace().level(0));
                    NameIdentifier nameIdentifier = NameIdentifier.of(ns, role);
                    reverseIndexCache.put(nameIdentifier, Entity.EntityType.ROLE, key);
                  });
        }
      };

  public static final ReverseIndexCache.ReverseIndexRule GROUP_OWNERSHIP_REVERSE_RULE =
      (entity, key, reverseIndexCache) -> {
        GroupEntity groupEntity = (GroupEntity) entity;
        // Handle Securable Objects -> Group reverse index, so the key type is group and the value
        // type is securable Object.
        if (key.relationType() == SupportsRelationOperations.Type.OWNER_REL) {
          reverseIndexCache.put(groupEntity.nameIdentifier(), EntityType.GROUP, key);
        }
      };

  /** RoleEntity reverse index processor */
  public static final ReverseIndexCache.ReverseIndexRule ROLE_SECURABLE_OBJECT_REVERSE_RULE =
      (entity, key, reverseIndexCache) -> {
        RoleEntity roleEntity = (RoleEntity) entity;
        if (roleEntity.securableObjects() != null) {
          roleEntity
              .securableObjects()
              .forEach(
                  securableObject -> {
                    Namespace namespace = Namespace.empty();
                    Entity.EntityType entityType = Entity.EntityType.METALAKE;
                    switch (securableObject.type()) {
                      case METALAKE:
                        entityType = Entity.EntityType.METALAKE;
                        namespace = NamespaceUtil.ofMetalake();
                        break;
                      case CATALOG:
                        entityType = Entity.EntityType.CATALOG;
                        namespace = NamespaceUtil.ofCatalog(roleEntity.namespace().level(0));
                        break;
                      case SCHEMA:
                        entityType = Entity.EntityType.SCHEMA;
                        Namespace nsSchema = Namespace.fromString(securableObject.parent());
                        namespace =
                            NamespaceUtil.ofSchema(
                                roleEntity.namespace().level(0), nsSchema.level(0));
                        break;
                      case TABLE:
                        entityType = Entity.EntityType.TABLE;
                        Namespace nsTable = Namespace.fromString(securableObject.parent());
                        namespace =
                            NamespaceUtil.ofTable(
                                roleEntity.namespace().level(0),
                                nsTable.level(0),
                                nsTable.level(1));
                        break;
                      case TOPIC:
                        entityType = Entity.EntityType.TOPIC;
                        Namespace nsTopic = Namespace.fromString(securableObject.parent());
                        namespace =
                            NamespaceUtil.ofTopic(
                                roleEntity.namespace().level(0),
                                nsTopic.level(0),
                                nsTopic.level(1));
                        break;
                      case MODEL:
                        entityType = Entity.EntityType.MODEL;
                        Namespace nsModel = Namespace.fromString(securableObject.parent());
                        namespace =
                            NamespaceUtil.ofModel(
                                roleEntity.namespace().level(0),
                                nsModel.level(0),
                                nsModel.level(1));
                        break;
                      case FILESET:
                        entityType = Entity.EntityType.FILESET;
                        Namespace nsFileset = Namespace.fromString(securableObject.parent());
                        namespace =
                            NamespaceUtil.ofFileset(
                                roleEntity.namespace().level(0),
                                nsFileset.level(0),
                                nsFileset.level(1));
                        break;
                      default:
                        throw new UnsupportedOperationException(
                            "Don't support securable object type: " + securableObject.type());
                    }
                    Namespace securableObjectNamespace = Namespace.of(namespace.levels());
                    NameIdentifier securableObjectIdent =
                        NameIdentifier.of(securableObjectNamespace, securableObject.name());
                    reverseIndexCache.put(securableObjectIdent, entityType, key);
                  });
        }
      };

  // Keep policies/tags to objects reverse index for metadata objects, so the key are objects and
  // the values are policies/tags.
  public static final ReverseIndexCache.ReverseIndexRule GENERIC_METADATA_OBJECT_REVERSE_RULE =
      (entity, key, reverseIndexCache) -> {
        // Name in GenericEntity contains no metalake.
        GenericEntity genericEntity = (GenericEntity) entity;
        EntityType type = entity.type();
        if (genericEntity.name() != null) {
          String[] levels = genericEntity.name().split("\\.");
          String metalakeName = key.identifier().namespace().levels()[0];
          NameIdentifier objectNameIdentifier =
              NameIdentifier.of(ArrayUtils.addFirst(levels, metalakeName));
          reverseIndexCache.put(objectNameIdentifier, type, key);
        }
      };

  // Keep objects to policies reverse index for policy objects, so the key are policies and the
  // values are objects.
  public static final ReverseIndexCache.ReverseIndexRule POLICY_SECURABLE_OBJECT_REVERSE_RULE =
      (entity, key, reverseIndexCache) -> {
        PolicyEntity policyEntity = (PolicyEntity) entity;
        NameIdentifier nameIdentifier =
            NameIdentifier.of(policyEntity.namespace(), policyEntity.name());
        reverseIndexCache.put(nameIdentifier, Entity.EntityType.POLICY, key);
      };

  // Keep objects to tags reverse index for tag objects, so the key are tags and the
  // values are objects.
  public static final ReverseIndexCache.ReverseIndexRule TAG_SECURABLE_OBJECT_REVERSE_RULE =
      (entity, key, reverseIndexCache) -> {
        TagEntity tagEntity = (TagEntity) entity;
        NameIdentifier nameIdentifier = NameIdentifier.of(tagEntity.namespace(), tagEntity.name());
        reverseIndexCache.put(nameIdentifier, Entity.EntityType.TAG, key);
      };
}
