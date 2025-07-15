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
package org.apache.gravitino.authorization;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;

/**
 * FutureGrantManager is responsible for granting privileges to future object. When you grant a
 * privilege which authorization supports to a metalake, the future creating catalog should apply
 * the privilege to underlying authorization plugin, too. FutureGrantManager selects the roles
 * contains the metalake securable object and filter unnecessary roles. Then, it selects the users
 * and groups by roles. Finally, it apply the information to the authorization plugins.
 */
public class FutureGrantManager {
  EntityStore entityStore;
  OwnerDispatcher ownerDispatcher;

  public FutureGrantManager(EntityStore entityStore, OwnerDispatcher ownerDispatcher) {
    this.entityStore = entityStore;
    this.ownerDispatcher = ownerDispatcher;
  }

  public void grantNewlyCreatedCatalog(String metalake, BaseCatalog catalog) {
    try {
      MetadataObject metalakeObject =
          MetadataObjects.of(null, metalake, MetadataObject.Type.METALAKE);
      Optional<Owner> ownerOptional = ownerDispatcher.getOwner(metalake, metalakeObject);
      ownerOptional.ifPresent(
          owner -> {
            AuthorizationPlugin authorizationPlugin = catalog.getAuthorizationPlugin();
            if (authorizationPlugin != null) {
              authorizationPlugin.onOwnerSet(metalakeObject, null, owner);
            }
          });

      Map<UserEntity, Set<RoleEntity>> userGrantRoles = Maps.newHashMap();
      Map<GroupEntity, Set<RoleEntity>> groupGrantRoles = Maps.newHashMap();
      List<RoleEntity> roles =
          entityStore.relationOperations()
              .listEntitiesByRelation(
                  SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
                  NameIdentifier.of(metalake),
                  Entity.EntityType.METALAKE)
              .stream()
              .map(entity -> (RoleEntity) entity)
              .collect(Collectors.toList());

      for (RoleEntity role : roles) {

        boolean supportsFutureGrant = false;
        for (SecurableObject object : role.securableObjects()) {
          if (AuthorizationUtils.needApplyAuthorizationPluginAllCatalogs(object)) {
            supportsFutureGrant = true;
            break;
          }
        }

        if (!supportsFutureGrant) {
          continue;
        }

        List<UserEntity> users =
            entityStore.relationOperations()
                .listEntitiesByRelation(
                    SupportsRelationOperations.Type.ROLE_USER_REL,
                    role.nameIdentifier(),
                    Entity.EntityType.ROLE)
                .stream()
                .map(entity -> (UserEntity) entity)
                .collect(Collectors.toList());

        for (UserEntity user : users) {
          Set<RoleEntity> roleSet = userGrantRoles.computeIfAbsent(user, k -> Sets.newHashSet());
          roleSet.add(role);
        }

        List<GroupEntity> groups =
            entityStore.relationOperations()
                .listEntitiesByRelation(
                    SupportsRelationOperations.Type.ROLE_GROUP_REL,
                    role.nameIdentifier(),
                    Entity.EntityType.ROLE)
                .stream()
                .map(entity -> (GroupEntity) entity)
                .collect(Collectors.toList());

        for (GroupEntity group : groups) {
          Set<RoleEntity> roleSet = groupGrantRoles.computeIfAbsent(group, k -> Sets.newHashSet());
          roleSet.add(role);
        }
      }

      for (Map.Entry<UserEntity, Set<RoleEntity>> entry : userGrantRoles.entrySet()) {
        AuthorizationPlugin authorizationPlugin = catalog.getAuthorizationPlugin();
        if (authorizationPlugin != null) {
          authorizationPlugin.onGrantedRolesToUser(
              Lists.newArrayList(entry.getValue()), entry.getKey());
        }
      }

      for (Map.Entry<GroupEntity, Set<RoleEntity>> entry : groupGrantRoles.entrySet()) {
        AuthorizationPlugin authorizationPlugin = catalog.getAuthorizationPlugin();

        if (authorizationPlugin != null) {
          authorizationPlugin.onGrantedRolesToGroup(
              Lists.newArrayList(entry.getValue()), entry.getKey());
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
