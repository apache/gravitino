/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.authorization.jcasbin;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.casbin.jcasbin.main.Enforcer;
import org.casbin.jcasbin.main.SyncedEnforcer;
import org.casbin.jcasbin.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Jcasbin implementation of GravitinoAuthorizer. */
public class JcasbinAuthorizer implements GravitinoAuthorizer {

  private static final Logger LOG = LoggerFactory.getLogger(JcasbinAuthorizer.class);

  /** Jcasbin enforcer is used for metadata authorization. */
  private Enforcer allowEnforcer;

  /** Jcasbin deny enforcer is used for metadata authorization. */
  private Enforcer denyEnforcer;

  /** allow internal authorizer */
  private InternalAuthorizer allowInternalAuthorizer;

  /** deny internal authorizer */
  private InternalAuthorizer denyInternalAuthorizer;

  /**
   * loadedRoles is used to cache roles that have loaded permissions. When the permissions of a role
   * are updated, they should be removed from it.
   */
  private Set<Long> loadedRoles = ConcurrentHashMap.newKeySet();

  @Override
  public void initialize() {
    allowEnforcer = new SyncedEnforcer(getModel("/jcasbin_model.conf"), new GravitinoAdapter());
    allowInternalAuthorizer = new InternalAuthorizer(allowEnforcer);
    denyEnforcer = new SyncedEnforcer(getModel("/jcasbin_model.conf"), new GravitinoAdapter());
    denyInternalAuthorizer = new InternalAuthorizer(denyEnforcer);
  }

  private Model getModel(String modelFilePath) {
    Model model = new Model();
    try (InputStream modelStream = JcasbinAuthorizer.class.getResourceAsStream(modelFilePath)) {
      Preconditions.checkNotNull(modelStream, "Jcasbin model file can not found.");
      String modelData = IOUtils.toString(modelStream, StandardCharsets.UTF_8);
      model.loadModelFromText(modelData);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return model;
  }

  @Override
  public boolean authorize(
      Principal principal,
      String metalake,
      MetadataObject metadataObject,
      Privilege.Name privilege) {
    boolean result =
        allowInternalAuthorizer.authorizeInternal(
            principal, metalake, metadataObject, privilege.name());
    LOG.debug(
        "principal {},metalake {},metadata object {},privilege {}, result {}",
        principal,
        metalake,
        metadataObject,
        privilege,
        result);
    return result;
  }

  @Override
  public boolean deny(
      Principal principal,
      String metalake,
      MetadataObject metadataObject,
      Privilege.Name privilege) {
    boolean result =
        denyInternalAuthorizer.authorizeInternal(
            principal, metalake, metadataObject, privilege.name());
    LOG.debug(
        "principal {},metalake {},metadata object {},privilege {},deny result {}",
        principal,
        metalake,
        metadataObject,
        privilege,
        result);
    return result;
  }

  @Override
  public boolean isOwner(Principal principal, String metalake, MetadataObject metadataObject) {
    boolean result =
        allowInternalAuthorizer.authorizeInternal(
            principal, metalake, metadataObject, AuthConstants.OWNER);
    LOG.debug(
        "principal {},metalake {},metadata object {},privilege {},deny result {}",
        principal,
        metalake,
        metadataObject,
        "OWNER",
        result);
    return result;
  }

  @Override
  public boolean isServiceAdmin() {
    return GravitinoEnv.getInstance()
        .accessControlDispatcher()
        .isServiceAdmin(PrincipalUtils.getCurrentUserName());
  }

  @Override
  public boolean isMetalakeUser(String metalake) {
    String currentUserName = PrincipalUtils.getCurrentUserName();
    if (StringUtils.isBlank(currentUserName)) {
      return false;
    }
    try {
      return GravitinoEnv.getInstance()
              .entityStore()
              .get(
                  NameIdentifierUtil.ofUser(metalake, currentUserName),
                  Entity.EntityType.USER,
                  UserEntity.class)
          != null;
    } catch (Exception e) {
      LOG.warn("Can not get user {} in metalake {}", currentUserName, metalake, e);
      return false;
    }
  }

  @Override
  public boolean isSelf(Entity.EntityType type, NameIdentifier nameIdentifier) {
    String metalake = nameIdentifier.namespace().level(0);
    String currentUserName = PrincipalUtils.getCurrentUserName();
    if (Entity.EntityType.USER == type) {
      return Objects.equals(nameIdentifier.name(), currentUserName);
    } else if (Entity.EntityType.ROLE == type) {
      try {
        Long roleId =
            MetadataIdConverter.getID(
                NameIdentifierUtil.toMetadataObject(nameIdentifier, type), metalake);
        UserEntity userEntity = getUserEntity(currentUserName, metalake);
        Long userId = userEntity.id();
        loadRolePrivilege(metalake, currentUserName, userId);
        return allowEnforcer.hasRoleForUser(String.valueOf(userId), String.valueOf(roleId));
      } catch (Exception e) {
        LOG.warn("can not get user id or role id.", e);
        return false;
      }
    }
    throw new UnsupportedOperationException("Unsupported Entity Type.");
  }

  @Override
  public boolean hasSetOwnerPermission(String metalake, String type, String fullName) {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    MetadataObject metalakeObject =
        MetadataObjects.of(ImmutableList.of(metalake), MetadataObject.Type.METALAKE);
    // metalake owner can set owner in metalake.
    if (isOwner(currentPrincipal, metalake, metalakeObject)) {
      return true;
    }
    MetadataObject.Type metadataType = MetadataObject.Type.valueOf(type.toUpperCase());
    MetadataObject metadataObject =
        MetadataObjects.of(Arrays.asList(fullName.split("\\.")), metadataType);
    do {
      if (isOwner(currentPrincipal, metalake, metadataObject)) {
        MetadataObject.Type tempType = metadataObject.type();
        if (tempType == MetadataObject.Type.SCHEMA) {
          // schema owner need use catalog privilege
          boolean hasCatalogUseCatalog =
              authorize(
                  currentPrincipal,
                  metalake,
                  MetadataObjects.parent(metadataObject),
                  Privilege.Name.USE_CATALOG);
          boolean hasMetalakeUseCatalog =
              authorize(currentPrincipal, metalake, metalakeObject, Privilege.Name.USE_CATALOG);
          return hasCatalogUseCatalog || hasMetalakeUseCatalog;
        }
        if (tempType == MetadataObject.Type.TABLE
            || tempType == MetadataObject.Type.TOPIC
            || tempType == MetadataObject.Type.FILESET
            || tempType == MetadataObject.Type.MODEL) {
          // table owner need use_catalog and use_schema privileges
          boolean hasMetalakeUseSchema =
              authorize(currentPrincipal, metalake, metalakeObject, Privilege.Name.USE_SCHEMA);
          MetadataObject schemaObject = MetadataObjects.parent(metadataObject);
          boolean hasCatalogUseSchema =
              authorize(
                  currentPrincipal,
                  metalake,
                  MetadataObjects.parent(schemaObject),
                  Privilege.Name.USE_SCHEMA);
          boolean hasSchemaUseSchema =
              authorize(currentPrincipal, metalake, schemaObject, Privilege.Name.USE_SCHEMA);
          return hasMetalakeUseSchema || hasCatalogUseSchema || hasSchemaUseSchema;
        }
        return true;
      }
      // metadata parent owner can set owner.
    } while ((metadataObject = MetadataObjects.parent(metadataObject)) != null);
    return false;
  }

  @Override
  public boolean hasMetadataPrivilegePermission(String metalake, String type, String fullName) {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    MetadataObject metalakeMetadataObject =
        MetadataObjects.of(ImmutableList.of(metalake), MetadataObject.Type.METALAKE);
    return authorize(
            currentPrincipal, metalake, metalakeMetadataObject, Privilege.Name.MANAGE_GRANTS)
        || hasSetOwnerPermission(metalake, type, fullName);
  }

  @Override
  public void handleRolePrivilegeChange(Long roleId) {
    loadedRoles.remove(roleId);
    allowEnforcer.deleteRole(String.valueOf(roleId));
    denyEnforcer.deleteRole(String.valueOf(roleId));
  }

  @Override
  public void handleMetadataOwnerChange(
      String metalake, Long oldOwnerId, NameIdentifier nameIdentifier, Entity.EntityType type) {
    MetadataObject metadataObject = NameIdentifierUtil.toMetadataObject(nameIdentifier, type);
    Long metadataId = MetadataIdConverter.getID(metadataObject, metalake);
    ImmutableList<String> policy =
        ImmutableList.of(
            String.valueOf(oldOwnerId),
            String.valueOf(metadataObject.type()),
            String.valueOf(metadataId),
            AuthConstants.OWNER,
            AuthConstants.ALLOW);
    allowEnforcer.removePolicy(policy);
  }

  @Override
  public void close() throws IOException {}

  private class InternalAuthorizer {

    Enforcer enforcer;

    public InternalAuthorizer(Enforcer enforcer) {
      this.enforcer = enforcer;
    }

    private boolean authorizeInternal(
        Principal principal, String metalake, MetadataObject metadataObject, String privilege) {
      String username = principal.getName();
      return loadPrivilegeAndAuthorize(username, metalake, metadataObject, privilege);
    }

    private boolean loadPrivilegeAndAuthorize(
        String username, String metalake, MetadataObject metadataObject, String privilege) {
      Long metadataId;
      Long userId;
      try {
        UserEntity userEntity = getUserEntity(username, metalake);
        userId = userEntity.id();
        metadataId = MetadataIdConverter.getID(metadataObject, metalake);
      } catch (Exception e) {
        LOG.debug("Can not get entity id", e);
        return false;
      }
      loadPrivilege(metalake, username, userId, metadataObject, metadataId);
      return authorizeByJcasbin(userId, metadataObject, metadataId, privilege);
    }

    private boolean authorizeByJcasbin(
        Long userId, MetadataObject metadataObject, Long metadataId, String privilege) {
      return enforcer.enforce(
          String.valueOf(userId),
          String.valueOf(metadataObject.type()),
          String.valueOf(metadataId),
          privilege);
    }
  }

  private static UserEntity getUserEntity(String username, String metalake) throws IOException {
    EntityStore entityStore = GravitinoEnv.getInstance().entityStore();
    UserEntity userEntity =
        entityStore.get(
            NameIdentifierUtil.ofUser(metalake, username),
            Entity.EntityType.USER,
            UserEntity.class);
    return userEntity;
  }

  private void loadPrivilege(
      String metalake,
      String username,
      Long userId,
      MetadataObject metadataObject,
      Long metadataObjectId) {
    try {
      loadRolePrivilege(metalake, username, userId);
      loadOwnerPolicy(metalake, metadataObject, metadataObjectId);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  private void loadRolePrivilege(String metalake, String username, Long userId) throws IOException {
    EntityStore entityStore = GravitinoEnv.getInstance().entityStore();
    NameIdentifier userNameIdentifier = NameIdentifierUtil.ofUser(metalake, username);
    List<RoleEntity> entities =
        entityStore
            .relationOperations()
            .listEntitiesByRelation(
                SupportsRelationOperations.Type.ROLE_USER_REL,
                userNameIdentifier,
                Entity.EntityType.USER);

    for (RoleEntity role : entities) {
      Long roleId = role.id();
      allowEnforcer.addRoleForUser(String.valueOf(userId), String.valueOf(roleId));
      denyEnforcer.addRoleForUser(String.valueOf(userId), String.valueOf(roleId));
      if (loadedRoles.contains(roleId)) {
        continue;
      }
      role =
          entityStore.get(
              NameIdentifierUtil.ofRole(metalake, role.name()),
              Entity.EntityType.ROLE,
              RoleEntity.class);

      loadPolicyByRoleEntity(role);
      loadedRoles.add(roleId);
    }
  }

  private void loadOwnerPolicy(String metalake, MetadataObject metadataObject, Long metadataId) {
    try {
      NameIdentifier entityIdent = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
      EntityStore entityStore = GravitinoEnv.getInstance().entityStore();
      List<? extends Entity> owners =
          entityStore
              .relationOperations()
              .listEntitiesByRelation(
                  SupportsRelationOperations.Type.OWNER_REL,
                  entityIdent,
                  Entity.EntityType.valueOf(metadataObject.type().name()));
      for (Entity ownerEntity : owners) {
        if (ownerEntity instanceof UserEntity) {
          UserEntity user = (UserEntity) ownerEntity;
          ImmutableList<String> policy =
              ImmutableList.of(
                  String.valueOf(user.id()),
                  String.valueOf(metadataObject.type()),
                  String.valueOf(metadataId),
                  AuthConstants.OWNER,
                  AuthConstants.ALLOW);
          allowEnforcer.addPolicy(policy);
        }
      }
    } catch (IOException e) {
      LOG.warn("Can not load metadata owner", e);
    }
  }

  private void loadPolicyByRoleEntity(RoleEntity roleEntity) {
    String metalake = NameIdentifierUtil.getMetalake(roleEntity.nameIdentifier());
    List<SecurableObject> securableObjects = roleEntity.securableObjects();

    for (SecurableObject securableObject : securableObjects) {
      for (Privilege privilege : securableObject.privileges()) {
        Privilege.Condition condition = privilege.condition();
        if (AuthConstants.DENY.equalsIgnoreCase(condition.name())) {
          denyEnforcer.addPolicy(
              String.valueOf(roleEntity.id()),
              securableObject.type().name(),
              String.valueOf(MetadataIdConverter.getID(securableObject, metalake)),
              privilege.name().name().toUpperCase(),
              AuthConstants.ALLOW);
        }
        allowEnforcer.addPolicy(
            String.valueOf(roleEntity.id()),
            securableObject.type().name(),
            String.valueOf(MetadataIdConverter.getID(securableObject, metalake)),
            privilege.name().name().toUpperCase(),
            condition.name().toLowerCase());
      }
    }
  }
}
