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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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
import org.apache.gravitino.UserGroup;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.catalog.HierarchicalSchemaUtil;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.meta.GroupEntity;
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
  private Cache<Long, Boolean> loadedRoles;

  private Cache<Long, Optional<OwnerInfo>> ownerRel;

  private Executor executor = null;

  @Override
  public void initialize() {
    long cacheExpirationSecs =
        GravitinoEnv.getInstance()
            .config()
            .get(Configs.GRAVITINO_AUTHORIZATION_CACHE_EXPIRATION_SECS);
    long roleCacheSize =
        GravitinoEnv.getInstance().config().get(Configs.GRAVITINO_AUTHORIZATION_ROLE_CACHE_SIZE);
    long ownerCacheSize =
        GravitinoEnv.getInstance().config().get(Configs.GRAVITINO_AUTHORIZATION_OWNER_CACHE_SIZE);

    // Initialize enforcers before the caches that reference them in removal listeners
    allowEnforcer = new SyncedEnforcer(getModel("/jcasbin_model.conf"), new GravitinoAdapter());
    allowInternalAuthorizer = new InternalAuthorizer(allowEnforcer);
    denyEnforcer = new SyncedEnforcer(getModel("/jcasbin_model.conf"), new GravitinoAdapter());
    denyInternalAuthorizer = new InternalAuthorizer(denyEnforcer);

    loadedRoles =
        Caffeine.newBuilder()
            .expireAfterAccess(cacheExpirationSecs, TimeUnit.SECONDS)
            .maximumSize(roleCacheSize)
            .executor(Runnable::run)
            .removalListener(
                (roleId, value, cause) -> {
                  if (roleId != null) {
                    allowEnforcer.deleteRole(String.valueOf(roleId));
                    denyEnforcer.deleteRole(String.valueOf(roleId));
                  }
                })
            .build();
    ownerRel =
        Caffeine.newBuilder()
            .expireAfterAccess(cacheExpirationSecs, TimeUnit.SECONDS)
            .maximumSize(ownerCacheSize)
            .build();
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
  }

  private Model getModel(String modelFilePath) {
    Model model = new Model();
    try (InputStream modelStream = JcasbinAuthorizer.class.getResourceAsStream(modelFilePath)) {
      Preconditions.checkArgument(modelStream != null, "Jcasbin model file can not found.");
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
        "Authorization expression: {},privilege {}, result {}\n, principal {},metalake {},metadata object {}",
        requestContext.getOriginalAuthorizationExpression(),
        privilege,
        result,
        principal,
        metalake,
        metadataObject);
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
        "Authorization expression: {},privilege {},deny result {}\n, principal {},metalake {},metadata object {}",
        requestContext.getOriginalAuthorizationExpression(),
        privilege,
        result,
        principal,
        metalake,
        metadataObject);
    return result;
  }

  @Override
  public boolean isOwner(
      Principal principal,
      String metalake,
      MetadataObject metadataObject,
      AuthorizationRequestContext requestContext) {
    boolean result = false;
    if (metadataObject == null) {
      return false;
    }

    if (metadataObject.type() == MetadataObject.Type.SCHEMA) {
      for (MetadataObject scopeObject : buildSchemaInheritanceChain(metadataObject)) {
        Long metadataId = MetadataIdConverter.getID(scopeObject, metalake);
        if (metadataId == null) {
          continue;
        }
        loadOwnerPolicy(metalake, scopeObject, metadataId);
        if (checkOwnership(principal, metalake, metadataId)) {
          result = true;
          break;
        }
      }
    } else {
      Long metadataId = MetadataIdConverter.getID(metadataObject, metalake);
      if (metadataId != null) {
        loadOwnerPolicy(metalake, metadataObject, metadataId);
        result = checkOwnership(principal, metalake, metadataId);
      }
    }

    LOG.debug(
        "Authorization expression: {},privilege {},owner result {}\n,principal {},metalake {},metadata object {}",
        requestContext.getOriginalAuthorizationExpression(),
        "OWNER",
        result,
        principal,
        metalake,
        metadataObject);
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
      return GravitinoEnv.getInstance().accessControlDispatcher().getUser(metalake, currentUserName)
          != null;
    } catch (NoSuchUserException e) {
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
    if (isOwner(currentPrincipal, metalake, metalakeObject, requestContext)) {
      return true;
    }
    MetadataObject.Type metadataType = MetadataObject.Type.valueOf(type.toUpperCase());
    MetadataObject metadataObject =
        MetadataObjects.of(Arrays.asList(fullName.split("\\.")), metadataType);
    do {
      if (isOwner(currentPrincipal, metalake, metadataObject, requestContext)) {
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
            || tempType == MetadataObject.Type.VIEW
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
    // Check whether the principal holds MANAGE_GRANTS on the target object or any ancestor.
    // A grant at a broader level (e.g. CATALOG or SCHEMA) implicitly covers all objects beneath it.
    MetadataObject.Type metadataType;
    try {
      metadataType = MetadataObject.Type.valueOf(type.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown metadata object type: " + type, e);
    }
    // Build the full ancestor chain from the target object up to and including the metalake.
    // MetadataObjects.parent(CATALOG) returns null (CATALOG is a root in the parent API), so the
    // metalake is appended manually at the end.
    List<MetadataObject> chain = new ArrayList<>();
    for (MetadataObject obj = MetadataObjects.parse(fullName, metadataType);
        obj != null;
        obj = MetadataObjects.parent(obj)) {
      chain.add(obj);
    }
    chain.add(MetadataObjects.of(ImmutableList.of(metalake), MetadataObject.Type.METALAKE));

    for (MetadataObject obj : chain) {
      if (authorize(
          currentPrincipal, metalake, obj, Privilege.Name.MANAGE_GRANTS, requestContext)) {
        return true;
      }
    }
    return hasSetOwnerPermission(metalake, type, fullName, requestContext);
  }

  @Override
  public void handleRolePrivilegeChange(Long roleId) {
    loadedRoles.invalidate(roleId);
  }

  @Override
  public void handleMetadataOwnerChange(
      String metalake, Long oldOwnerId, NameIdentifier nameIdentifier, Entity.EntityType type) {
    MetadataObject metadataObject = NameIdentifierUtil.toMetadataObject(nameIdentifier, type);
    Long metadataId = MetadataIdConverter.getID(metadataObject, metalake);
    ownerRel.invalidate(metadataId);
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

  /**
   * Builds the logical schema inheritance chain for a SCHEMA MetadataObject. For a schema named
   * {@code "catalog.A:B:C"} this returns MetadataObjects for {@code catalog.A:B:C}, {@code
   * catalog.A:B}, and {@code catalog.A} in that order.
   *
   * <p>For flat (non-nested) schemas the list contains only the original object.
   */
  private List<MetadataObject> buildSchemaInheritanceChain(MetadataObject schemaObject) {
    String separator = HierarchicalSchemaUtil.namespaceSeparator();

    List<MetadataObject> chain = new ArrayList<>();
    for (String scope : HierarchicalSchemaUtil.allScopes(schemaObject.name(), separator)) {
      chain.add(MetadataObjects.of(schemaObject.parent(), scope, MetadataObject.Type.SCHEMA));
    }
    return ImmutableList.copyOf(chain);
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
      Long userId;
      try {
        UserEntity userEntity = getUserEntity(username, metalake);
        userId = userEntity.id();
      } catch (Exception e) {
        LOG.debug("Can not get entity id", e);
        return false;
      }
      loadRolePrivilege(metalake, username, userId, requestContext);

      // when checking CREATE SCHEMA, the metadata object may be null, we can just
      // skip the check
      if (metadataObject == null) {
        return false;
      }
      // For SCHEMA objects with hierarchical names (e.g. "catalog.A:B:C"), walk the logical
      // parent chain so that a privilege granted on an ancestor schema is inherited by all
      // descendant schemas.
      if (metadataObject.type() == MetadataObject.Type.SCHEMA) {
        List<MetadataObject> chain = buildSchemaInheritanceChain(metadataObject);
        for (MetadataObject scopeObject : chain) {
          Long metadataId = MetadataIdConverter.getID(scopeObject, metalake);
          if (metadataId != null
              && authorizeByJcasbin(userId, metalake, scopeObject, metadataId, privilege)) {
            return true;
          }
        }
        return false;
      }

      Long metadataId;

      metadataId = MetadataIdConverter.getID(metadataObject, metalake);

      return metadataId != null
          && authorizeByJcasbin(userId, metalake, metadataObject, metadataId, privilege);
    }

    private boolean authorizeByJcasbin(
        Long userId,
        String metalake,
        MetadataObject metadataObject,
        Long metadataId,
        String privilege) {
      if (AuthConstants.OWNER.equals(privilege)) {
        return checkOwnership(PrincipalUtils.getCurrentPrincipal(), metalake, metadataId);
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
              if (loadedRoles.getIfPresent(roleId) != null) {
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
                            loadedRoles.put(roleId, true);
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
    if (ownerRel.getIfPresent(metadataId) != null) {
      LOG.debug("Metadata {} OWNER has been loaded.", metadataId);
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
      if (owners.isEmpty()) {
        ownerRel.put(metadataId, Optional.empty());
      } else {
        for (Entity ownerEntity : owners) {
          if (ownerEntity instanceof UserEntity) {
            UserEntity user = (UserEntity) ownerEntity;
            ownerRel.put(
                metadataId,
                Optional.of(new OwnerInfo(user.id(), Entity.EntityType.USER, user.name())));
          } else if (ownerEntity instanceof GroupEntity) {
            GroupEntity group = (GroupEntity) ownerEntity;
            ownerRel.put(
                metadataId,
                Optional.of(new OwnerInfo(group.id(), Entity.EntityType.GROUP, group.name())));
          }
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
              AuthorizationUtils.replaceLegacyPrivilegeName(privilege.name())
                  .name()
                  .toUpperCase(java.util.Locale.ROOT),
              AuthConstants.ALLOW);
        }
        // Since different roles of a user may simultaneously hold both "allow" and "deny"
        // permissions
        // for the same privilege on a given MetadataObject, the allowEnforcer must also incorporate
        // the "deny" privilege to ensure that the authorize method correctly returns false in such
        // cases. For example, if role1 has an "allow" privilege for SELECT_TABLE on table1, while
        // role2 has a "deny" privilege for the same action on table1, then a user assigned both
        // roles should receive a false result when calling the authorize method.

        allowEnforcer.addPolicy(
            String.valueOf(roleEntity.id()),
            securableObject.type().name(),
            String.valueOf(MetadataIdConverter.getID(securableObject, metalake)),
            AuthorizationUtils.replaceLegacyPrivilegeName(privilege.name())
                .name()
                .toUpperCase(java.util.Locale.ROOT),
            condition.name().toLowerCase(java.util.Locale.ROOT));
      }
    }
  }

  /**
   * Checks whether the given principal is the owner of the metadata object identified by
   * metadataId. Supports both user and group ownership.
   */
  private boolean checkOwnership(Principal principal, String metalake, Long metadataId) {
    Optional<OwnerInfo> ownerOpt = ownerRel.getIfPresent(metadataId);
    if (ownerOpt == null || !ownerOpt.isPresent()) {
      return false;
    }
    OwnerInfo owner = ownerOpt.get();
    // We compare by entity ID rather than name to guard against stale cache entries.
    // If a user/group is deleted and recreated with the same name, the cached OwnerInfo
    // still holds the old ID. A name-only comparison would incorrectly grant ownership
    // to the new entity. The extra IO to fetch the current entity ensures correctness.
    if (owner.type == Entity.EntityType.USER) {
      try {
        UserEntity userEntity = getUserEntity(principal.getName(), metalake);
        return Objects.equals(userEntity.id(), owner.id);
      } catch (Exception e) {
        LOG.debug("Can not get user entity for ownership check", e);
        return false;
      }
    } else if (owner.type == Entity.EntityType.GROUP) {
      if (principal instanceof UserPrincipal) {
        List<UserGroup> groups = ((UserPrincipal) principal).getGroups();
        if (groups.isEmpty()) {
          return false;
        }
        try {
          List<NameIdentifier> groupIdents =
              groups.stream()
                  .map(g -> NameIdentifierUtil.ofGroup(metalake, g.getGroupname()))
                  .collect(Collectors.toList());
          List<GroupEntity> groupEntities =
              GravitinoEnv.getInstance()
                  .entityStore()
                  .batchGet(groupIdents, Entity.EntityType.GROUP, GroupEntity.class);
          return groupEntities.stream().anyMatch(ge -> Objects.equals(ge.id(), owner.id));
        } catch (Exception e) {
          LOG.debug("Can not get group entities for ownership check", e);
          return false;
        }
      }
      return false;
    }
    return false;
  }

  /** Holds the owner identity for a metadata object in the owner cache. */
  static class OwnerInfo {
    final Long id;
    final Entity.EntityType type;
    final String name;

    OwnerInfo(Long id, Entity.EntityType type, String name) {
      this.id = id;
      this.type = type;
      this.name = name;
    }
  }
}
