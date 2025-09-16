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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
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

  /**
   * loadedOwners is used to cache owners that have loaded permissions. When the permissions of a
   * role are updated, they should be removed from it.
   */
  private Set<Long> loadedOwners = ConcurrentHashMap.newKeySet();

  private Map<Long, Long> ownerRel = new ConcurrentHashMap<>();

  private Executor executor = null;

  @Override
  public void initialize() {
    executor =
        Executors.newFixedThreadPool(
            GravitinoEnv.getInstance()
                .config()
                .get(Configs.GRAVITINO_AUTHORIZATION_THREAD_POOL_SIZE),
            runnable -> {
              Thread thread = new Thread(runnable);
              thread.setName("GravitinoAuthorizer-ThreadPool-" + thread.getId());
              return thread;
            });
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
      Privilege.Name privilege,
      AuthorizationRequestContext requestContext) {
    boolean result =
        requestContext.authorizeAllow(
            principal,
            metalake,
            metadataObject,
            privilege,
            (authorizationKey) ->
                allowInternalAuthorizer.authorizeInternal(
                    authorizationKey.getPrincipal().getName(),
                    authorizationKey.getMetalake(),
                    authorizationKey.getMetadataObject(),
                    authorizationKey.getPrivilege().name(),
                    requestContext));
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
      Privilege.Name privilege,
      AuthorizationRequestContext requestContext) {
    boolean result =
        requestContext.authorizeDeny(
            principal,
            metalake,
            metadataObject,
            privilege,
            (authorizationKey) ->
                denyInternalAuthorizer.authorizeInternal(
                    authorizationKey.getPrincipal().getName(),
                    authorizationKey.getMetalake(),
                    authorizationKey.getMetadataObject(),
                    authorizationKey.getPrivilege().name(),
                    requestContext));
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
    Long userId;
    boolean result;
    try {
      Long metadataId = MetadataIdConverter.getID(metadataObject, metalake);
      loadOwnerPolicy(metalake, metadataObject, metadataId);
      UserEntity userEntity = getUserEntity(principal.getName(), metalake);
      userId = userEntity.id();
      metadataId = MetadataIdConverter.getID(metadataObject, metalake);
      result = Objects.equals(userId, ownerRel.get(metadataId));
    } catch (Exception e) {
      LOG.debug("Can not get entity id", e);
      result = false;
    }
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
        EntityStore entityStore = GravitinoEnv.getInstance().entityStore();
        NameIdentifier userNameIdentifier =
            NameIdentifierUtil.ofUser(metalake, PrincipalUtils.getCurrentUserName());
        List<RoleEntity> entities =
            entityStore
                .relationOperations()
                .listEntitiesByRelation(
                    SupportsRelationOperations.Type.ROLE_USER_REL,
                    userNameIdentifier,
                    Entity.EntityType.USER);
        return entities.stream().anyMatch(roleEntity -> Objects.equals(roleEntity.id(), roleId));

      } catch (Exception e) {
        LOG.warn("can not get user id or role id.", e);
        return false;
      }
    }
    throw new UnsupportedOperationException("Unsupported Entity Type.");
  }

  @Override
  public boolean hasSetOwnerPermission(
      String metalake, String type, String fullName, AuthorizationRequestContext requestContext) {
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
                  Privilege.Name.USE_CATALOG,
                  requestContext);
          boolean hasMetalakeUseCatalog =
              authorize(
                  currentPrincipal,
                  metalake,
                  metalakeObject,
                  Privilege.Name.USE_CATALOG,
                  requestContext);
          return hasCatalogUseCatalog || hasMetalakeUseCatalog;
        }
        if (tempType == MetadataObject.Type.TABLE
            || tempType == MetadataObject.Type.TOPIC
            || tempType == MetadataObject.Type.FILESET
            || tempType == MetadataObject.Type.MODEL) {
          // table owner need use_catalog and use_schema privileges
          boolean hasMetalakeUseSchema =
              authorize(
                  currentPrincipal,
                  metalake,
                  metalakeObject,
                  Privilege.Name.USE_SCHEMA,
                  requestContext);
          MetadataObject schemaObject = MetadataObjects.parent(metadataObject);
          boolean hasCatalogUseSchema =
              authorize(
                  currentPrincipal,
                  metalake,
                  MetadataObjects.parent(schemaObject),
                  Privilege.Name.USE_SCHEMA,
                  requestContext);
          boolean hasSchemaUseSchema =
              authorize(
                  currentPrincipal,
                  metalake,
                  schemaObject,
                  Privilege.Name.USE_SCHEMA,
                  requestContext);
          return hasMetalakeUseSchema || hasCatalogUseSchema || hasSchemaUseSchema;
        }
        return true;
      }
      // metadata parent owner can set owner.
    } while ((metadataObject = MetadataObjects.parent(metadataObject)) != null);
    return false;
  }

  @Override
  public boolean hasMetadataPrivilegePermission(
      String metalake, String type, String fullName, AuthorizationRequestContext requestContext) {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    MetadataObject metalakeMetadataObject =
        MetadataObjects.of(ImmutableList.of(metalake), MetadataObject.Type.METALAKE);
    return authorize(
            currentPrincipal,
            metalake,
            metalakeMetadataObject,
            Privilege.Name.MANAGE_GRANTS,
            requestContext)
        || hasSetOwnerPermission(metalake, type, fullName, requestContext);
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
    ownerRel.remove(metadataId);
    loadedOwners.remove(metadataId);
  }

  @Override
  public void close() throws IOException {
    if (executor != null) {
      if (executor instanceof ThreadPoolExecutor) {
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executor;
        threadPoolExecutor.shutdown();
      }
    }
  }

  private class InternalAuthorizer {

    Enforcer enforcer;

    public InternalAuthorizer(Enforcer enforcer) {
      this.enforcer = enforcer;
    }

    private boolean authorizeInternal(
        String username,
        String metalake,
        MetadataObject metadataObject,
        String privilege,
        AuthorizationRequestContext requestContext) {
      return loadPrivilegeAndAuthorize(
          username, metalake, metadataObject, privilege, requestContext);
    }

    private boolean loadPrivilegeAndAuthorize(
        String username,
        String metalake,
        MetadataObject metadataObject,
        String privilege,
        AuthorizationRequestContext requestContext) {
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
      loadRolePrivilege(metalake, username, userId, requestContext);
      return authorizeByJcasbin(userId, metadataObject, metadataId, privilege);
    }

    private boolean authorizeByJcasbin(
        Long userId, MetadataObject metadataObject, Long metadataId, String privilege) {
      if (AuthConstants.OWNER.equals(privilege)) {
        return Objects.equals(userId, ownerRel.get(metadataId));
      }
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

  private void loadRolePrivilege(
      String metalake, String username, Long userId, AuthorizationRequestContext requestContext) {
    requestContext.loadRole(
        () -> {
          EntityStore entityStore = GravitinoEnv.getInstance().entityStore();
          NameIdentifier userNameIdentifier = NameIdentifierUtil.ofUser(metalake, username);
          List<RoleEntity> entities;
          try {
            entities =
                entityStore
                    .relationOperations()
                    .listEntitiesByRelation(
                        SupportsRelationOperations.Type.ROLE_USER_REL,
                        userNameIdentifier,
                        Entity.EntityType.USER);
            List<CompletableFuture<Void>> loadRoleFutures = new ArrayList<>();
            for (RoleEntity role : entities) {
              Long roleId = role.id();
              allowEnforcer.addRoleForUser(String.valueOf(userId), String.valueOf(roleId));
              denyEnforcer.addRoleForUser(String.valueOf(userId), String.valueOf(roleId));
              if (loadedRoles.contains(roleId)) {
                continue;
              }
              CompletableFuture<Void> loadRoleFuture =
                  CompletableFuture.supplyAsync(
                          () -> {
                            try {
                              return entityStore.get(
                                  NameIdentifierUtil.ofRole(metalake, role.name()),
                                  Entity.EntityType.ROLE,
                                  RoleEntity.class);
                            } catch (Exception e) {
                              throw new RuntimeException("Failed to load role: " + role.name(), e);
                            }
                          },
                          executor)
                      .thenAcceptAsync(
                          roleEntity -> {
                            loadPolicyByRoleEntity(roleEntity);
                            loadedRoles.add(roleId);
                          },
                          executor);
              loadRoleFutures.add(loadRoleFuture);
            }
            CompletableFuture.allOf(loadRoleFutures.toArray(new CompletableFuture[0])).join();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  private void loadOwnerPolicy(String metalake, MetadataObject metadataObject, Long metadataId) {
    if (loadedOwners.contains(metadataId)) {
      LOG.debug("Metadata {} OWNER has bean loaded.", metadataId);
      return;
    }
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
          ownerRel.put(metadataId, user.id());
          loadedOwners.add(metadataId);
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
