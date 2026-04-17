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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.cache.CaffeineGravitinoCache;
import org.apache.gravitino.cache.GravitinoCache;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.RoleMetaMapper;
import org.apache.gravitino.storage.relational.mapper.UserMetaMapper;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.gravitino.storage.relational.po.auth.ChangedOwnerInfo;
import org.apache.gravitino.storage.relational.po.auth.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.po.auth.RoleUpdatedAt;
import org.apache.gravitino.storage.relational.po.auth.UserAuthInfo;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
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

  /** Key separator for hierarchical cache keys. */
  static final String KEY_SEP = "::";

  /** Max rows to fetch per poller cycle. */
  private static final int POLLER_MAX_ROWS = 500;

  /** Jcasbin enforcer is used for metadata authorization. */
  private Enforcer allowEnforcer;

  /** Jcasbin deny enforcer is used for metadata authorization. */
  private Enforcer denyEnforcer;

  /** allow internal authorizer */
  private InternalAuthorizer allowInternalAuthorizer;

  /** deny internal authorizer */
  private InternalAuthorizer denyInternalAuthorizer;

  // ---- Version-validated caches (strong consistency) ----

  /**
   * userRoleCache: metalake::userName -> CachedUserRoles. Version-validated per request via
   * user_meta.updated_at.
   */
  private GravitinoCache<String, CachedUserRoles> userRoleCache;

  // TODO: Phase 2 — add groupRoleCache: GravitinoCache<String, CachedGroupRoles>
  //  for group-role version-validated caching (metalake::groupId -> CachedGroupRoles).

  /**
   * loadedRoles: roleId -> updated_at. If the DB updated_at is newer, evict and reload policies.
   */
  private GravitinoCache<Long, Long> loadedRoles;

  // ---- Eventual consistency caches (poller-driven) ----

  /**
   * metadataIdCache: hierarchical key (metalake::catalog::schema::table::TYPE) -> entity id.
   * Evicted by entity change poller.
   */
  private GravitinoCache<String, Long> metadataIdCache;

  /** ownerRelCache: metadataObjectId -> Optional(ownerId). Evicted by owner change poller. */
  private GravitinoCache<Long, Optional<Long>> ownerRelCache;

  /** Scheduled poller for owner and entity structural changes. */
  private ScheduledExecutorService changePoller;

  /** High-water marks for pollers. */
  private volatile long ownerPollHighWaterMark = 0;

  private volatile long entityPollHighWaterMark = 0;

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
    long metadataIdCacheSize =
        GravitinoEnv.getInstance()
            .config()
            .get(Configs.GRAVITINO_AUTHORIZATION_METADATA_ID_CACHE_SIZE);
    long pollIntervalSecs =
        GravitinoEnv.getInstance()
            .config()
            .get(Configs.GRAVITINO_AUTHORIZATION_CHANGE_POLL_INTERVAL_SECS);

    long ttlMs = cacheExpirationSecs * 1000L;

    // Initialize enforcers before caches that reference them in removal listeners
    allowEnforcer = new SyncedEnforcer(getModel("/jcasbin_model.conf"), new GravitinoAdapter());
    allowInternalAuthorizer = new InternalAuthorizer(allowEnforcer);
    denyEnforcer = new SyncedEnforcer(getModel("/jcasbin_model.conf"), new GravitinoAdapter());
    denyInternalAuthorizer = new InternalAuthorizer(denyEnforcer);

    // loadedRoles: roleId -> updated_at.
    // When evicted, we must clean up the corresponding JCasbin policies.
    loadedRoles = new LoadedRolesCache(ttlMs, roleCacheSize, allowEnforcer, denyEnforcer);

    userRoleCache = new CaffeineGravitinoCache<>(ttlMs, roleCacheSize);
    metadataIdCache = new CaffeineGravitinoCache<>(ttlMs, metadataIdCacheSize);
    ownerRelCache = new CaffeineGravitinoCache<>(ttlMs, ownerCacheSize);

    // Initialize high-water marks to current time so we only pick up future changes
    long now = System.currentTimeMillis();
    ownerPollHighWaterMark = now;
    entityPollHighWaterMark = now;

    // Start the change poller
    changePoller =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r);
              t.setName("GravitinoAuthorizer-ChangePoller");
              t.setDaemon(true);
              return t;
            });
    changePoller.scheduleWithFixedDelay(
        this::pollChanges, pollIntervalSecs, pollIntervalSecs, TimeUnit.SECONDS);
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

  // ---------------------------------------------------------------------------
  //  Authorize / deny / isOwner
  // ---------------------------------------------------------------------------

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
    boolean result;
    try {
      Long metadataId = resolveMetadataId(metadataObject, metalake);
      loadOwnerPolicy(metadataId);
      UserEntity userEntity = getUserEntity(principal.getName(), metalake);
      Long userId = userEntity.id();
      Optional<Optional<Long>> ownerOpt = ownerRelCache.getIfPresent(metadataId);
      result = ownerOpt.isPresent() && Objects.equals(Optional.of(userId), ownerOpt.get());
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
                    org.apache.gravitino.SupportsRelationOperations.Type.ROLE_USER_REL,
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
    MetadataObject.Type metadataType = MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT));
    MetadataObject metadataObject =
        MetadataObjects.of(Arrays.asList(fullName.split("\\.")), metadataType);
    do {
      if (isOwner(currentPrincipal, metalake, metadataObject, requestContext)) {
        MetadataObject.Type tempType = metadataObject.type();
        if (tempType == MetadataObject.Type.SCHEMA) {
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
    } while ((metadataObject = MetadataObjects.parent(metadataObject)) != null);
    return false;
  }

  @Override
  public boolean hasMetadataPrivilegePermission(
      String metalake, String type, String fullName, AuthorizationRequestContext requestContext) {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    MetadataObject.Type metadataType;
    try {
      metadataType = MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown metadata object type: " + type, e);
    }
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

  // ---------------------------------------------------------------------------
  //  Cache invalidation hooks (called from service layer)
  // ---------------------------------------------------------------------------

  @Override
  public void handleRolePrivilegeChange(Long roleId) {
    loadedRoles.invalidate(roleId);
  }

  @Override
  public void handleMetadataOwnerChange(
      String metalake, Long oldOwnerId, NameIdentifier nameIdentifier, Entity.EntityType type) {
    MetadataObject metadataObject = NameIdentifierUtil.toMetadataObject(nameIdentifier, type);
    Long metadataId = MetadataIdConverter.getID(metadataObject, metalake);
    ownerRelCache.invalidate(metadataId);
    // Owner mutations may happen after drop/recreate with the same name. Invalidate the
    // name->id mapping as well to prevent using a stale metadataId from metadataIdCache.
    metadataIdCache.invalidate(buildCacheKey(metalake, metadataObject));
  }

  @Override
  public void handleEntityStructuralChange(
      String metalake, NameIdentifier nameIdentifier, Entity.EntityType type) {
    MetadataObject metadataObject = NameIdentifierUtil.toMetadataObject(nameIdentifier, type);
    String cacheKey = buildCacheKey(metalake, metadataObject);
    if (isNonLeaf(metadataObject.type())) {
      // Cascade invalidation: metalake::catalog:: prefix removes catalog + all children
      metadataIdCache.invalidateByPrefix(cacheKey);
    } else {
      metadataIdCache.invalidate(cacheKey);
    }
  }

  @Override
  public void close() throws IOException {
    if (changePoller != null) {
      changePoller.shutdown();
      try {
        if (!changePoller.awaitTermination(5, TimeUnit.SECONDS)) {
          changePoller.shutdownNow();
        }
      } catch (InterruptedException e) {
        changePoller.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
    if (userRoleCache != null) {
      userRoleCache.close();
    }
    if (loadedRoles != null) {
      loadedRoles.close();
    }
    if (metadataIdCache != null) {
      metadataIdCache.close();
    }
    if (ownerRelCache != null) {
      ownerRelCache.close();
    }
  }

  // ---------------------------------------------------------------------------
  //  Internal authorizer
  // ---------------------------------------------------------------------------

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
      Long metadataId;
      long userId;
      UserAuthInfo userInfo;
      try {
        // Step 1a: lightweight query — get userId + user.updated_at (version sentinel)
        //          This is the ONLY place getUserInfo is called per request.
        userInfo =
            SessionUtils.getWithoutCommit(
                UserMetaMapper.class, m -> m.getUserInfo(metalake, username));
        if (userInfo == null) {
          LOG.debug("User {} not found in metalake {}", username, metalake);
          return false;
        }
        userId = userInfo.getUserId();

        // Step 2: resolve metadata name → id via metadataIdCache (cache hit when warm)
        metadataId = resolveMetadataId(metadataObject, metalake);
      } catch (Exception e) {
        LOG.debug("Can not get entity id", e);
        return false;
      }

      // Steps 1b→3: version-validated role loading — pass userInfo to avoid re-query
      loadRolePrivilege(metalake, username, userId, userInfo, requestContext);

      // Step 4: JCasbin enforce (pure in-memory)
      if (AuthConstants.OWNER.equals(privilege)) {
        Optional<Optional<Long>> ownerOpt = ownerRelCache.getIfPresent(metadataId);
        return ownerOpt.isPresent() && Objects.equals(Optional.of(userId), ownerOpt.get());
      }
      return enforcer.enforce(
          String.valueOf(userId),
          String.valueOf(metadataObject.type()),
          String.valueOf(metadataId),
          privilege);
    }
  }

  // ---------------------------------------------------------------------------
  //  Metadata ID resolution with cache
  // ---------------------------------------------------------------------------

  private Long resolveMetadataId(MetadataObject metadataObject, String metalake) {
    String cacheKey = buildCacheKey(metalake, metadataObject);
    Optional<Long> cached = metadataIdCache.getIfPresent(cacheKey);
    if (cached.isPresent()) {
      return cached.get();
    }
    Long id = MetadataIdConverter.getID(metadataObject, metalake);
    metadataIdCache.put(cacheKey, id);
    return id;
  }

  // ---------------------------------------------------------------------------
  //  4-step role loading with version validation
  // ---------------------------------------------------------------------------

  private void loadRolePrivilege(
      String metalake,
      String username,
      long userId,
      UserAuthInfo userInfo,
      AuthorizationRequestContext requestContext) {
    requestContext.loadRole(
        () -> {
          // Step 1a: version-validate user role cache (userInfo already fetched in
          // authorizeInternal)
          List<Long> userRoleIds = loadUserRoles(metalake, username, userId, userInfo);

          // TODO: Step 1b — load group-role mappings via groupRoleCache (Phase 2).
          //  Current code only handles user-direct roles. Group role support will be
          //  added in a follow-up iteration using getGroupInfoByUserId(userId) +
          //  groupRoleCache version validation.

          // Step 3: batch version-check all role IDs, load stale ones (1 query)
          if (!userRoleIds.isEmpty()) {
            versionCheckAndLoadRoles(metalake, userRoleIds);
          }
        });
  }

  private List<Long> loadUserRoles(
      String metalake, String username, long userId, UserAuthInfo userInfo) {
    String userCacheKey = metalake + KEY_SEP + username;
    Optional<CachedUserRoles> cachedOpt = userRoleCache.getIfPresent(userCacheKey);

    if (cachedOpt.isPresent() && cachedOpt.get().getUpdatedAt() >= userInfo.getUpdatedAt()) {
      // Cache is still valid
      CachedUserRoles cached = cachedOpt.get();
      bindUserRoles(userId, cached.getRoleIds());
      return cached.getRoleIds();
    }

    // Cache miss or stale — reload from DB
    List<RolePO> rolePOs =
        SessionUtils.getWithoutCommit(RoleMetaMapper.class, m -> m.listRolesByUserId(userId));
    List<Long> roleIds = rolePOs.stream().map(RolePO::getRoleId).collect(Collectors.toList());

    userRoleCache.put(userCacheKey, new CachedUserRoles(userId, userInfo.getUpdatedAt(), roleIds));
    bindUserRoles(userId, roleIds);
    return roleIds;
  }

  private void versionCheckAndLoadRoles(String metalake, List<Long> roleIds) {
    // Step 3: batch fetch (roleId, roleName, updated_at) for all role IDs — 1 query
    List<Long> uniqueRoleIds = roleIds.stream().distinct().collect(Collectors.toList());
    List<RoleUpdatedAt> roleVersions =
        SessionUtils.getWithoutCommit(
            RoleMetaMapper.class, m -> m.batchGetUpdatedAt(uniqueRoleIds));

    for (RoleUpdatedAt rv : roleVersions) {
      long roleId = rv.getRoleId();
      long dbUpdatedAt = rv.getUpdatedAt();
      Optional<Long> cachedUpdatedAt = loadedRoles.getIfPresent(roleId);

      if (cachedUpdatedAt.isPresent() && cachedUpdatedAt.get() >= dbUpdatedAt) {
        // Role policies are still current
        continue;
      }

      // Stale or missing — evict old policies and reload
      if (cachedUpdatedAt.isPresent()) {
        allowEnforcer.deleteRole(String.valueOf(roleId));
        denyEnforcer.deleteRole(String.valueOf(roleId));
      }

      // Load full role entity using roleName from the batch query (no extra DB scan)
      try {
        EntityStore entityStore = GravitinoEnv.getInstance().entityStore();
        RoleEntity roleEntity =
            entityStore.get(
                NameIdentifierUtil.ofRole(metalake, rv.getRoleName()),
                Entity.EntityType.ROLE,
                RoleEntity.class);
        loadPolicyByRoleEntity(roleEntity);
      } catch (Exception e) {
        LOG.warn("Failed to load role policies for roleId {}", roleId, e);
        continue;
      }

      loadedRoles.put(roleId, dbUpdatedAt);
    }
  }

  private void bindUserRoles(long userId, List<Long> roleIds) {
    for (Long roleId : roleIds) {
      allowEnforcer.addRoleForUser(String.valueOf(userId), String.valueOf(roleId));
      denyEnforcer.addRoleForUser(String.valueOf(userId), String.valueOf(roleId));
    }
  }

  // ---------------------------------------------------------------------------
  //  Owner loading
  // ---------------------------------------------------------------------------

  private void loadOwnerPolicy(Long metadataId) {
    if (ownerRelCache.getIfPresent(metadataId).isPresent()) {
      LOG.debug("Metadata {} OWNER has been loaded.", metadataId);
      return;
    }

    OwnerInfo ownerInfo =
        SessionUtils.getWithoutCommit(
            OwnerMetaMapper.class, m -> m.selectOwnerByMetadataObjectId(metadataId));
    if (ownerInfo == null) {
      ownerRelCache.put(metadataId, Optional.empty());
    } else {
      ownerRelCache.put(metadataId, Optional.of(ownerInfo.getOwnerId()));
    }
  }

  // ---------------------------------------------------------------------------
  //  Policy loading from role entity
  // ---------------------------------------------------------------------------

  private void loadPolicyByRoleEntity(RoleEntity roleEntity) {
    String metalake = NameIdentifierUtil.getMetalake(roleEntity.nameIdentifier());
    List<SecurableObject> securableObjects = roleEntity.securableObjects();

    for (SecurableObject securableObject : securableObjects) {
      Long securableId = resolveMetadataId(securableObject, metalake);
      for (Privilege privilege : securableObject.privileges()) {
        Privilege.Condition condition = privilege.condition();
        if (AuthConstants.DENY.equalsIgnoreCase(condition.name())) {
          denyEnforcer.addPolicy(
              String.valueOf(roleEntity.id()),
              securableObject.type().name(),
              String.valueOf(securableId),
              AuthorizationUtils.replaceLegacyPrivilegeName(privilege.name())
                  .name()
                  .toUpperCase(Locale.ROOT),
              AuthConstants.ALLOW);
        }

        allowEnforcer.addPolicy(
            String.valueOf(roleEntity.id()),
            securableObject.type().name(),
            String.valueOf(securableId),
            AuthorizationUtils.replaceLegacyPrivilegeName(privilege.name())
                .name()
                .toUpperCase(Locale.ROOT),
            condition.name().toLowerCase(Locale.ROOT));
      }
    }
  }

  // ---------------------------------------------------------------------------
  //  Change poller (eventual consistency for HA)
  // ---------------------------------------------------------------------------

  @VisibleForTesting
  void pollChanges() {
    try {
      LOG.debug("Polling for owner changes since {}", ownerPollHighWaterMark);
      pollOwnerChanges();
    } catch (Exception e) {
      LOG.warn("Owner change poll failed", e);
    }

    try {
      LOG.debug("Polling for entity changes since {}", entityPollHighWaterMark);
      pollEntityChanges();
    } catch (Exception e) {
      LOG.warn("Entity change poll failed", e);
    }
  }

  private void pollOwnerChanges() {
    List<ChangedOwnerInfo> changes =
        SessionUtils.getWithoutCommit(
            OwnerMetaMapper.class, m -> m.selectChangedOwners(ownerPollHighWaterMark));

    long maxSeen = ownerPollHighWaterMark;
    for (ChangedOwnerInfo change : changes) {
      ownerRelCache.invalidate(change.getMetadataObjectId());
      if (change.getUpdatedAt() > maxSeen) {
        maxSeen = change.getUpdatedAt();
      }
    }
    ownerPollHighWaterMark = maxSeen;
  }

  private void pollEntityChanges() {
    List<EntityChangeRecord> changes =
        SessionUtils.getWithoutCommit(
            EntityChangeLogMapper.class,
            m -> m.selectChanges(entityPollHighWaterMark, POLLER_MAX_ROWS));

    long maxSeen = entityPollHighWaterMark;
    for (EntityChangeRecord change : changes) {
      String metalake = change.getMetalakeName();
      String entityType = change.getEntityType();
      String fullName = change.getFullName();

      MetadataObject.Type mdType;
      try {
        mdType = MetadataObject.Type.valueOf(entityType.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        LOG.warn("Unknown entity type in change log: {}", entityType);
        if (change.getCreatedAt() > maxSeen) {
          maxSeen = change.getCreatedAt();
        }
        continue;
      }

      MetadataObject mdObj = MetadataObjects.of(Arrays.asList(fullName.split("\\.")), mdType);
      String cacheKey = buildCacheKey(metalake, mdObj);

      if (isNonLeaf(mdType)) {
        metadataIdCache.invalidateByPrefix(cacheKey);
      } else {
        metadataIdCache.invalidate(cacheKey);
      }

      if (change.getCreatedAt() > maxSeen) {
        maxSeen = change.getCreatedAt();
      }
    }
    entityPollHighWaterMark = maxSeen;
  }

  // ---------------------------------------------------------------------------
  //  Helpers
  // ---------------------------------------------------------------------------

  /**
   * Builds a hierarchical cache key for the metadataIdCache. Non-leaf objects end with "::" to
   * enable prefix-based cascade invalidation.
   *
   * <p>Examples: metalake::catalog:: , metalake::catalog::schema:: ,
   * metalake::catalog::schema::table::TABLE
   */
  @VisibleForTesting
  static String buildCacheKey(String metalake, MetadataObject metadataObject) {
    StringBuilder sb = new StringBuilder(metalake);
    sb.append(KEY_SEP);
    // fullName uses '.' as separator, e.g. "catalog1.schema1.table1"
    String[] parts = metadataObject.fullName().split("\\.");
    sb.append(String.join(KEY_SEP, parts));
    if (isNonLeaf(metadataObject.type())) {
      // Trailing separator enables prefix-based cascade invalidation
      sb.append(KEY_SEP);
    } else {
      // Leaf nodes get the type suffix to avoid collisions
      sb.append(KEY_SEP);
      sb.append(metadataObject.type().name());
    }
    return sb.toString();
  }

  /** Returns true for entity types that can contain children (metalake, catalog, schema). */
  @VisibleForTesting
  static boolean isNonLeaf(MetadataObject.Type type) {
    return type == MetadataObject.Type.METALAKE
        || type == MetadataObject.Type.CATALOG
        || type == MetadataObject.Type.SCHEMA;
  }

  private static UserEntity getUserEntity(String username, String metalake) throws IOException {
    EntityStore entityStore = GravitinoEnv.getInstance().entityStore();
    return entityStore.get(
        NameIdentifierUtil.ofUser(metalake, username), Entity.EntityType.USER, UserEntity.class);
  }

  // ---------------------------------------------------------------------------
  //  LoadedRoles cache — wraps CaffeineGravitinoCache with eviction side-effects
  // ---------------------------------------------------------------------------

  /**
   * A specialized GravitinoCache for loaded roles that cleans up JCasbin policies on eviction. This
   * uses a raw Caffeine cache internally so that we can attach a removal listener.
   */
  private static class LoadedRolesCache implements GravitinoCache<Long, Long> {

    private final Cache<Long, Long> cache;

    LoadedRolesCache(long ttlMs, long maxSize, Enforcer allowEnforcer, Enforcer denyEnforcer) {
      this.cache =
          com.github.benmanes.caffeine.cache.Caffeine.newBuilder()
              .expireAfterAccess(ttlMs, TimeUnit.MILLISECONDS)
              .maximumSize(maxSize)
              .executor(Runnable::run)
              .removalListener(
                  (roleId, value, cause) -> {
                    if (roleId != null) {
                      allowEnforcer.deleteRole(String.valueOf(roleId));
                      denyEnforcer.deleteRole(String.valueOf(roleId));
                    }
                  })
              .build();
    }

    @Override
    public Optional<Long> getIfPresent(Long key) {
      Long v = cache.getIfPresent(key);
      return Optional.ofNullable(v);
    }

    @Override
    public void put(Long key, Long value) {
      cache.put(key, value);
    }

    @Override
    public void invalidate(Long key) {
      cache.invalidate(key);
    }

    @Override
    public void invalidateAll() {
      cache.invalidateAll();
    }

    @Override
    public void invalidateByPrefix(String prefix) {
      cache.asMap().keySet().removeIf(k -> k.toString().startsWith(prefix));
    }

    @Override
    public long size() {
      cache.cleanUp();
      return cache.estimatedSize();
    }

    @Override
    public void close() {
      cache.invalidateAll();
      cache.cleanUp();
    }
  }
}
