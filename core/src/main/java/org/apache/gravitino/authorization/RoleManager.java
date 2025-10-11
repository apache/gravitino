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

import static org.apache.gravitino.metalake.MetalakeManager.checkMetalake;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.RoleAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RoleManager is responsible for managing the roles. Role contains the privileges of one privilege
 * entity. If one Role is created and the privilege entity is an external system, the role will be
 * created in the underlying entity, too.
 */
class RoleManager {

  private static final Logger LOG = LoggerFactory.getLogger(RoleManager.class);
  private final EntityStore store;
  private final IdGenerator idGenerator;

  RoleManager(EntityStore store, IdGenerator idGenerator) {
    this.store = store;
    this.idGenerator = idGenerator;
  }

  RoleEntity createRole(
      String metalake,
      String role,
      Map<String, String> properties,
      List<SecurableObject> securableObjects)
      throws RoleAlreadyExistsException {
    checkMetalake(NameIdentifier.of(metalake), store);
    RoleEntity roleEntity =
        RoleEntity.builder()
            .withId(idGenerator.nextId())
            .withName(role)
            .withProperties(properties)
            .withSecurableObjects(securableObjects)
            .withNamespace(AuthorizationUtils.ofRoleNamespace(metalake))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    try {
      store.put(roleEntity, false /* overwritten */);

      AuthorizationUtils.callAuthorizationPluginForSecurableObjects(
          metalake,
          roleEntity.securableObjects(),
          (authorizationPlugin, catalogName) ->
              authorizationPlugin.onRoleCreated(
                  AuthorizationUtils.filterSecurableObjects(roleEntity, metalake, catalogName)));

      return roleEntity;
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("Role {} in the metalake {} already exists", role, metalake, e);
      throw new RoleAlreadyExistsException(
          "Role %s in the metalake %s already exists", role, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Creating role {} failed in the metalake {} due to storage issues", role, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  RoleEntity getRole(String metalake, String role) throws NoSuchRoleException {
    try {
      checkMetalake(NameIdentifier.of(metalake), store);
      return getRoleEntity(AuthorizationUtils.ofRole(metalake, role));
    } catch (NoSuchEntityException e) {
      LOG.warn("Role {} does not exist in the metalake {}", role, metalake, e);
      throw new NoSuchRoleException(AuthorizationUtils.ROLE_DOES_NOT_EXIST_MSG, role, metalake);
    }
  }

  boolean deleteRole(String metalake, String role) {
    try {
      checkMetalake(NameIdentifier.of(metalake), store);
      NameIdentifier ident = AuthorizationUtils.ofRole(metalake, role);

      try {
        RoleEntity roleEntity = store.get(ident, Entity.EntityType.ROLE, RoleEntity.class);
        AuthorizationUtils.callAuthorizationPluginForSecurableObjects(
            metalake,
            roleEntity.securableObjects(),
            (authorizationPlugin, catalogName) ->
                authorizationPlugin.onRoleDeleted(
                    AuthorizationUtils.filterSecurableObjects(roleEntity, metalake, catalogName)));
      } catch (NoSuchEntityException nse) {
        // ignore, because the role may have been deleted.
      }

      return store.delete(ident, Entity.EntityType.ROLE);
    } catch (IOException ioe) {
      LOG.error(
          "Deleting role {} in the metalake {} failed due to storage issues", role, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  String[] listRoleNames(String metalake) {
    try {
      checkMetalake(NameIdentifier.of(metalake), store);
      Namespace namespace = AuthorizationUtils.ofRoleNamespace(metalake);
      return store.list(namespace, RoleEntity.class, Entity.EntityType.ROLE).stream()
          .map(Role::name)
          .toArray(String[]::new);
    } catch (IOException ioe) {
      LOG.error("Listing user under metalake {} failed due to storage issues", metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  String[] listRoleNamesByObject(String metalake, MetadataObject object) {
    try {
      checkMetalake(NameIdentifier.of(metalake), store);

      return store
          .relationOperations()
          .listEntitiesByRelation(
              SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
              MetadataObjectUtil.toEntityIdent(metalake, object),
              MetadataObjectUtil.toEntityType(object),
              false /* allFields */)
          .stream()
          .map(entity -> ((RoleEntity) entity).name())
          .toArray(String[]::new);

    } catch (NoSuchEntityException nse) {
      LOG.error("Metadata object {} (type {}) doesn't exist", object.fullName(), object.type());
      throw new NoSuchMetadataObjectException(
          "Metadata object %s (type %s) doesn't exist", object.fullName(), object.type());
    } catch (IOException ioe) {
      LOG.error(
          "Listing roles under metalake {} by object full name {} and type {} failed due to storage issues",
          metalake,
          object.fullName(),
          object.type(),
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  private RoleEntity getRoleEntity(NameIdentifier identifier) {
    try {
      return store.get(identifier, Entity.EntityType.ROLE, RoleEntity.class);
    } catch (IOException ioe) {
      LOG.error("Failed to get roles {} due to storage issues", identifier, ioe);
      throw new RuntimeException(ioe);
    }
  }
}
