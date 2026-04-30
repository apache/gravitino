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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.exceptions.NoSuchUserException;
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
   * loadedRoles caches the indexed privileges for each loaded role. When a role's privileges are
   * updated, the role should be removed from this cache.
   */
  private Cache<Long, Map<PolicyKey, Effect>> loadedRoles;

  private Cache<Long, Optional<Long>> ownerRel;

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
    allowInternalAuthorizer = new InternalAuthorizer(allowEnforcer, AuthorizationMode.ALLOW);
    denyEnforcer = new SyncedEnforcer(getModel("/jcasbin_model.conf"), new GravitinoAdapter());
    denyInternalAuthorizer = new InternalAuthorizer(denyEnforcer, AuthorizationMode.DENY);

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
    Long userId;
    boolean result;
    try {
      Long metadataId = MetadataIdConverter.getID(metadataObject, metalake);
      loadOwnerPolicy(metalake, metadataObject, metadataId);
      UserEntity userEntity = getUserEntity(principal.getName(), metalake);
      userId = userEntity.id();
      metadataId = MetadataIdConverter.getID(metadataObject, metalake);
      result = Objects.equals(Optional.of(userId), ownerRel.getIfPresent(metadataId));
    } catch (Exception e) {
      LOG.debug("Can not get entity id", e);
      result = false;
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

  private class InternalAuthorizer {

    private final Enforcer enforcer;
    private final AuthorizationMode authorizationMode;

    InternalAuthorizer(Enforcer enforcer, AuthorizationMode authorizationMode) {
      this.enforcer = enforcer;
      this.authorizationMode = authorizationMode;
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
      return authorizeByIndex(userId, metadataObject, metadataId, privilege, requestContext);
    }

    /**
     * Resolve a single privilege probe against the per-role policy index. Replaces the previous
     * {@code enforcer.enforce} call, which scanned every policy line in the enforcer for each
     * probe. Per-request cost goes from {@code O(total_policies)} to {@code O(roles_per_user)} hash
     * probes.
     */
    private boolean authorizeByIndex(
        Long userId,
        MetadataObject metadataObject,
        Long metadataId,
        String privilege,
        AuthorizationRequestContext requestContext) {
      if (AuthConstants.OWNER.equals(privilege)) {
        Optional<Long> owner = ownerRel.getIfPresent(metadataId);
        return Objects.equals(Optional.of(userId), owner);
      }
      Set<Long> roleIds = requestContext.getUserRoleIds();
      if (roleIds.isEmpty()) {
        return false;
      }
      PolicyKey key = new PolicyKey(metadataObject.type().name(), metadataId, privilege);
      if (authorizationMode == AuthorizationMode.DENY) {
        for (Long roleId : roleIds) {
          Map<PolicyKey, Effect> idx = loadedRoles.getIfPresent(roleId);
          if (idx != null && idx.get(key) == Effect.DENY) {
            return true;
          }
        }
        return false;
      }
      boolean allow = false;
      for (Long roleId : roleIds) {
        Map<PolicyKey, Effect> idx = loadedRoles.getIfPresent(roleId);
        if (idx == null) {
          continue;
        }
        Effect effect = idx.get(key);
        if (effect == Effect.DENY) {
          return false;
        }
        if (effect == Effect.ALLOW) {
          allow = true;
        }
      }
      return allow;
    }

    /** Retained so reflection-based tests that touch the underlying enforcer keep working. */
    Enforcer getEnforcer() {
      return enforcer;
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
            Set<Long> roleIds = new HashSet<>(entities.size());
            List<CompletableFuture<Void>> loadRoleFutures = new ArrayList<>();
            for (RoleEntity role : entities) {
              Long roleId = role.id();
              roleIds.add(roleId);
              allowEnforcer.addRoleForUser(String.valueOf(userId), String.valueOf(roleId));
              denyEnforcer.addRoleForUser(String.valueOf(userId), String.valueOf(roleId));
              if (loadedRoles.getIfPresent(roleId) != null) {
                continue;
              }
              CompletableFuture<Void> loadRoleFuture =
                  CompletableFuture.runAsync(
                      () -> {
                        loadedRoles.get(
                            roleId,
                            unused -> {
                              try {
                                RoleEntity roleEntity =
                                    entityStore.get(
                                        NameIdentifierUtil.ofRole(metalake, role.name()),
                                        Entity.EntityType.ROLE,
                                        RoleEntity.class);
                                return loadPolicyByRoleEntity(roleEntity);
                              } catch (Exception e) {
                                throw new RuntimeException(
                                    "Failed to load role: " + role.name(), e);
                              }
                            });
                      },
                      executor);
              loadRoleFutures.add(loadRoleFuture);
            }
            CompletableFuture.allOf(loadRoleFutures.toArray(new CompletableFuture[0])).join();
            requestContext.setUserRoleIds(roleIds);
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
            ownerRel.put(metadataId, Optional.of(user.id()));
          }
        }
      }
    } catch (IOException e) {
      LOG.warn("Can not load metadata owner", e);
    }
  }

  private Map<PolicyKey, Effect> loadPolicyByRoleEntity(RoleEntity roleEntity) {
    String metalake = NameIdentifierUtil.getMetalake(roleEntity.nameIdentifier());
    List<SecurableObject> securableObjects = roleEntity.securableObjects();
    Long roleId = roleEntity.id();
    String roleIdStr = String.valueOf(roleId);
    Map<PolicyKey, Effect> index = new ConcurrentHashMap<>();

    for (SecurableObject securableObject : securableObjects) {
      Long metadataId = MetadataIdConverter.getID(securableObject, metalake);
      String metadataIdStr = String.valueOf(metadataId);
      String typeName = securableObject.type().name();
      for (Privilege privilege : securableObject.privileges()) {
        Privilege.Condition condition = privilege.condition();
        String privilegeName =
            AuthorizationUtils.replaceLegacyPrivilegeName(privilege.name())
                .name()
                .toUpperCase(Locale.ROOT);
        boolean isDeny = AuthConstants.DENY.equalsIgnoreCase(condition.name());
        if (isDeny) {
          denyEnforcer.addPolicy(
              roleIdStr, typeName, metadataIdStr, privilegeName, AuthConstants.ALLOW);
        }
        // Since different roles of a user may simultaneously hold both "allow" and "deny"
        // permissions
        // for the same privilege on a given MetadataObject, the allowEnforcer must also incorporate
        // the "deny" privilege to ensure that the authorize method correctly returns false in such
        // cases. For example, if role1 has an "allow" privilege for SELECT_TABLE on table1, while
        // role2 has a "deny" privilege for the same action on table1, then a user assigned both
        // roles should receive a false result when calling the authorize method.

        allowEnforcer.addPolicy(
            roleIdStr,
            typeName,
            metadataIdStr,
            privilegeName,
            condition.name().toLowerCase(Locale.ROOT));

        // Populate the per-role index. Within a single role DENY wins over ALLOW so that the
        // index agrees with the allowEnforcer's policy_effect (some allow && !some deny).
        PolicyKey key = new PolicyKey(typeName, metadataId, privilegeName);
        Effect effect = isDeny ? Effect.DENY : Effect.ALLOW;
        index.merge(
            key, effect, (existing, incoming) -> existing == Effect.DENY ? existing : incoming);
      }
    }
    return index;
  }

  /** Composite key for the per-role policy index. */
  static final class PolicyKey {
    private final String type;
    private final Long metadataId;
    private final String privilege;
    private final int hash;

    PolicyKey(String type, Long metadataId, String privilege) {
      this.type = type;
      this.metadataId = metadataId;
      this.privilege = privilege;
      this.hash = Objects.hash(type, metadataId, privilege);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof PolicyKey)) {
        return false;
      }
      PolicyKey other = (PolicyKey) o;
      return hash == other.hash
          && Objects.equals(metadataId, other.metadataId)
          && Objects.equals(type, other.type)
          && Objects.equals(privilege, other.privilege);
    }

    @Override
    public int hashCode() {
      return hash;
    }
  }

  /** Per-role per-key effect; DENY beats ALLOW within a role and across roles. */
  enum Effect {
    ALLOW,
    DENY
  }

  private enum AuthorizationMode {
    ALLOW,
    DENY
  }
}
