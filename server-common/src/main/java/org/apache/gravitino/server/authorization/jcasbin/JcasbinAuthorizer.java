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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
import org.apache.gravitino.cache.CaffeineGravitinoCache;
import org.apache.gravitino.cache.GravitinoCache;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.storage.relational.mapper.GroupMetaMapper;
import org.apache.gravitino.storage.relational.mapper.RoleMetaMapper;
import org.apache.gravitino.storage.relational.mapper.UserMetaMapper;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.gravitino.storage.relational.po.auth.AuthSubjectVersion;
import org.apache.gravitino.storage.relational.po.auth.GroupUpdatedAt;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.po.auth.RoleUpdatedAt;
import org.apache.gravitino.storage.relational.po.auth.UserUpdatedAt;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.casbin.jcasbin.main.Enforcer;
import org.casbin.jcasbin.main.SyncedEnforcer;
import org.casbin.jcasbin.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Jcasbin implementation of {@link GravitinoAuthorizer}.
 *
 * <h2>Cache architecture</h2>
 *
 * <p>Authorization decisions are read-mostly and run on the hot path, so this class layers three
 * cache families with different consistency models:
 *
 * <ol>
 *   <li><b>Per-request dedup</b> — fields on {@link AuthorizationRequestContext} (user info, group
 *       info, name→id, owner). A fresh context is created for every HTTP request; every underlying
 *       DB query runs at most once per request even when the same authorize/isOwner pair is
 *       evaluated repeatedly for a single authorization expression.
 *   <li><b>Version-validated shared caches</b> (strong consistency) — {@link #userRoleCache},
 *       {@link #groupRoleCache}, {@link #loadedRoles}. Each cached entry carries the {@code
 *       *_meta.updated_at} value it was loaded against; every read issues a lightweight version
 *       probe and discards the entry if the DB sentinel has advanced. No TTL is relied on for
 *       correctness — {@code expireAfterAccess} only bounds memory.
 *   <li><b>Eventual-consistency caches</b> — {@link #metadataIdCache} and {@link #ownerRelCache}. A
 *       single background poller ({@link #changePoller}) drains {@code entity_change_log} and
 *       {@code owner_meta} change rows since a high-water-mark cursor and invalidates the affected
 *       keys. Other Gravitino nodes therefore observe ALTER/DROP and owner changes within one poll
 *       interval.
 * </ol>
 *
 * <p>The pollers are best-effort and intentionally cheap; see {@link JcasbinChangePoller} for the
 * contracts they rely on (most notably that {@code entity_change_log.full_name} is the pre-mutation
 * name).
 *
 * <p>JCasbin enforcer state ({@link #allowEnforcer}/{@link #denyEnforcer}) is kept in sync with
 * {@link #loadedRoles} via the removal listener inside {@link JcasbinLoadedRolesCache} — evicting a
 * role id also deletes that role's policies from both enforcers.
 */
public class JcasbinAuthorizer implements GravitinoAuthorizer {

  private static final Logger LOG = LoggerFactory.getLogger(JcasbinAuthorizer.class);

  /** Key separator for hierarchical cache keys. */
  static final String KEY_SEP = "::";

  /**
   * {@code subjectType} literal column value returned by {@link
   * UserMetaMapper#batchGetUserAndGroupUpdatedAt} for user-meta rows.
   */
  private static final String SUBJECT_TYPE_USER = "USER";

  /**
   * {@code subjectType} literal column value returned by {@link
   * UserMetaMapper#batchGetUserAndGroupUpdatedAt} for group-meta rows.
   */
  private static final String SUBJECT_TYPE_GROUP = "GROUP";

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

  /**
   * groupRoleCache: metalake::groupName -> CachedGroupRoles. Version-validated per request via
   * group_meta.updated_at.
   */
  private GravitinoCache<String, CachedGroupRoles> groupRoleCache;

  /**
   * loadedRoles: roleId -> updated_at. If the DB updated_at is newer, evict and reload policies.
   */
  private GravitinoCache<Long, Long> loadedRoles;

  // ---- Eventual consistency caches (poller-driven) ----

  /**
   * Path-based key {@code metalake::catalog::schema::object::TYPE} -> entity id. Evicted by entity
   * change poller.
   */
  private GravitinoCache<String, Long> metadataIdCache;

  /** ownerRelCache: metadataObjectId -> Optional(owner). Evicted by owner change poller. */
  private GravitinoCache<Long, Optional<OwnerInfo>> ownerRelCache;

  /** Two-tier lookup facade for metadata-id / owner (per-request dedup + Caffeine + DB). */
  private JcasbinAuthorizationLookups lookups;

  /** Background HA invalidator for {@link #metadataIdCache} and {@link #ownerRelCache}. */
  private JcasbinChangePoller changePoller;

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

    long ttlMs = TimeUnit.SECONDS.toMillis(cacheExpirationSecs);

    // Initialize enforcers before caches that reference them in removal listeners
    allowEnforcer = new SyncedEnforcer(getModel("/jcasbin_model.conf"), new GravitinoAdapter());
    allowInternalAuthorizer = new InternalAuthorizer(allowEnforcer);
    denyEnforcer = new SyncedEnforcer(getModel("/jcasbin_model.conf"), new GravitinoAdapter());
    denyInternalAuthorizer = new InternalAuthorizer(denyEnforcer);

    // loadedRoles: roleId -> updated_at.
    // When evicted, we must clean up the corresponding JCasbin policies.
    loadedRoles = new JcasbinLoadedRolesCache(ttlMs, roleCacheSize, allowEnforcer, denyEnforcer);

    userRoleCache = new CaffeineGravitinoCache<>(ttlMs, roleCacheSize);
    groupRoleCache = new CaffeineGravitinoCache<>(ttlMs, roleCacheSize);
    // The change poller is the primary HA invalidation path. These write-based TTLs bound the
    // stale window if a poll cycle misses a change; access-based TTLs could keep hot stale entries
    // alive indefinitely.
    metadataIdCache = new CaffeineGravitinoCache<>(ttlMs, metadataIdCacheSize);
    ownerRelCache = new CaffeineGravitinoCache<>(ttlMs, ownerCacheSize);
    lookups = new JcasbinAuthorizationLookups(metadataIdCache, ownerRelCache);
    changePoller = new JcasbinChangePoller(metadataIdCache, ownerRelCache, pollIntervalSecs);
    changePoller.start();
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
      Long metadataId = lookups.resolveMetadataId(metadataObject, metalake, requestContext);
      Optional<UserUpdatedAt> userInfoOpt =
          loadUserInfo(metalake, principal.getName(), requestContext);
      if (!userInfoOpt.isPresent()) {
        result = false;
      } else {
        Optional<OwnerInfo> owner =
            lookups.resolveOwnerId(metadataId, metadataObject.type(), requestContext);
        result = ownerMatchesUserOrGroups(owner, userInfoOpt.get().getUserId(), metalake);
      }
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
  public boolean isMetalakeUser(String metalake, AuthorizationRequestContext requestContext) {
    String currentUserName = PrincipalUtils.getCurrentUserName();
    if (StringUtils.isBlank(currentUserName)) {
      return false;
    }
    // Reuse the per-request UserUpdatedAt cache populated by authorize/isOwner. Presence of a
    // UserUpdatedAt entry for (metalake, user) already implies the user exists in that metalake,
    // so we avoid a second accessControlDispatcher().getUser() DB round-trip per request.
    return loadUserInfo(metalake, currentUserName, requestContext).isPresent();
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
        // Check direct user-role assignment
        if (entities.stream().anyMatch(roleEntity -> Objects.equals(roleEntity.id(), roleId))) {
          return true;
        }
        // Check group-role assignments.
        for (GroupEntity groupEntity : resolveCurrentUserGroups(metalake, entityStore)) {
          List<Long> groupRoleIds = groupEntity.roleIds();
          if (groupRoleIds != null && groupRoleIds.contains(roleId)) {
            return true;
          }
        }
        return false;

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
    // Owner mutations may happen after drop/recreate with the same name. Invalidate the
    // name->id mapping as well to prevent using a stale metadataId from metadataIdCache.
    metadataIdCache.invalidate(JcasbinAuthorizationLookups.buildCacheKey(metalake, metadataObject));
    try {
      Long metadataId = MetadataIdConverter.getID(metadataObject, metalake);
      ownerRelCache.invalidate(metadataId);
    } catch (RuntimeException e) {
      LOG.warn("Failed to resolve metadata id for owner cache invalidation: {}", metadataObject, e);
    }
  }

  @Override
  public void handleEntityNameIdMappingChange(
      String metalake, NameIdentifier nameIdentifier, Entity.EntityType type) {
    MetadataObject metadataObject = NameIdentifierUtil.toMetadataObject(nameIdentifier, type);
    String cacheKey = JcasbinAuthorizationLookups.buildCacheKey(metalake, metadataObject);
    if (JcasbinAuthorizationLookups.isContainerType(metadataObject.type())) {
      // Prefix invalidation: metalake::catalog:: removes catalog + all children.
      metadataIdCache.invalidateByPrefix(cacheKey);
    } else {
      metadataIdCache.invalidate(cacheKey);
    }
  }

  @Override
  public void close() throws IOException {
    if (changePoller != null) {
      changePoller.close();
    }
    if (userRoleCache != null) {
      userRoleCache.close();
    }
    if (groupRoleCache != null) {
      groupRoleCache.close();
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
      UserUpdatedAt userInfo;
      try {
        // Step 1a: lightweight UNION query — get userId + user.updated_at AND every group's
        //          version in one round trip. Subsequent loadUserInfo / loadGroupInfo calls in
        //          this request hit the per-request cache (0 DB queries).
        Optional<UserUpdatedAt> userInfoOpt =
            prefetchUserAndGroupInfo(
                metalake, username, currentPrincipalGroupNames(), requestContext);
        if (!userInfoOpt.isPresent()) {
          LOG.debug("User {} not found in metalake {}", username, metalake);
          return false;
        }
        userInfo = userInfoOpt.get();
        userId = userInfo.getUserId();

        // Step 2: resolve metadata name → id via metadataIdCache (cache hit when warm)
        metadataId = lookups.resolveMetadataId(metadataObject, metalake, requestContext);
      } catch (Exception e) {
        LOG.debug("Can not get entity id", e);
        return false;
      }

      // Steps 1b→3: version-validated role loading — pass userInfo to avoid re-query
      loadRolePrivilege(metalake, username, userId, userInfo, requestContext);

      // Step 4: JCasbin enforce (pure in-memory)
      if (AuthConstants.OWNER.equals(privilege)) {
        // Cold-path: resolveOwnerId loads from DB when neither the per-request nor the shared
        // Caffeine cache has the entry, ensuring the first OWNER check doesn't spuriously deny.
        Optional<OwnerInfo> owner =
            lookups.resolveOwnerId(metadataId, metadataObject.type(), requestContext);
        return ownerMatchesUserOrGroups(owner, userId, metalake);
      }
      return enforcer.enforce(
          String.valueOf(userId),
          String.valueOf(metadataObject.type()),
          String.valueOf(metadataId),
          privilege);
    }
  }

  // ---------------------------------------------------------------------------
  //  User info / ownership helpers
  // ---------------------------------------------------------------------------

  /**
   * Per-request {@link UserUpdatedAt} lookup. The underlying {@code user_meta} query is issued at
   * most once per (metalake, username) within a single request.
   */
  private Optional<UserUpdatedAt> loadUserInfo(
      String metalake, String username, AuthorizationRequestContext requestContext) {
    String cacheKey = metalake + KEY_SEP + username;
    return requestContext.computeUserInfoIfAbsent(
        cacheKey,
        k ->
            Optional.ofNullable(
                SessionUtils.getWithoutCommit(
                    UserMetaMapper.class, m -> m.getUserUpdatedAt(metalake, username))));
  }

  /**
   * Batched version probe that collapses {@link #loadUserInfo} and a per-group {@link
   * #loadGroupInfo} fan-out into a single UNION query. After this returns, both the per-request
   * {@code userInfoCache} and {@code groupInfoCache} on {@code requestContext} are primed: every
   * requested group key is present (mapped to either a {@link GroupUpdatedAt} when the row exists
   * or {@link Optional#empty()} when the IdP-pushed name is stale).
   *
   * <p>Saves {@code N} round trips on the hot path: where the legacy flow paid {@code 1 user_meta +
   * N group_meta} probes, this pays {@code 1} UNION. {@code N=0} (principal has no groups) degrades
   * to a plain user-only SELECT via the SQL provider.
   *
   * <p>{@code computeXxxIfAbsent} is used to populate the caches so that an entry already cached
   * from a prior call in the same request is not overwritten — matching {@link #loadUserInfo}'s and
   * {@link #loadGroupInfo}'s "first wins" dedup semantics.
   */
  private Optional<UserUpdatedAt> prefetchUserAndGroupInfo(
      String metalake,
      String username,
      List<String> groupNames,
      AuthorizationRequestContext requestContext) {

    List<AuthSubjectVersion> rows =
        SessionUtils.getWithoutCommit(
            UserMetaMapper.class,
            m -> m.batchGetUserAndGroupUpdatedAt(metalake, username, groupNames));

    UserUpdatedAt foundUser = null;
    Map<String, GroupUpdatedAt> foundGroups = new HashMap<>();
    for (AuthSubjectVersion row : rows) {
      if (SUBJECT_TYPE_USER.equals(row.getSubjectType())) {
        foundUser = new UserUpdatedAt(row.getId(), row.getUpdatedAt());
      } else if (SUBJECT_TYPE_GROUP.equals(row.getSubjectType())) {
        foundGroups.put(row.getName(), new GroupUpdatedAt(row.getId(), row.getUpdatedAt()));
      }
    }

    String userKey = metalake + KEY_SEP + username;
    final Optional<UserUpdatedAt> userValue = Optional.ofNullable(foundUser);
    Optional<UserUpdatedAt> userOpt =
        requestContext.computeUserInfoIfAbsent(userKey, k -> userValue);

    // Negative-cache absent groups too so loadGroupRoles can short-circuit without re-querying.
    for (String groupName : groupNames) {
      String groupKey = metalake + KEY_SEP + groupName;
      final Optional<GroupUpdatedAt> groupValue = Optional.ofNullable(foundGroups.get(groupName));
      requestContext.computeGroupInfoIfAbsent(groupKey, k -> groupValue);
    }

    return userOpt;
  }

  /**
   * Returns true when the cached owner type and ID match the current user or one of the user's
   * groups.
   */
  private boolean ownerMatchesUserOrGroups(
      Optional<OwnerInfo> owner, long userId, String metalake) {
    if (!owner.isPresent()) {
      return false;
    }
    OwnerInfo ownerInfo = owner.get();
    if (Entity.EntityType.USER.name().equalsIgnoreCase(ownerInfo.getOwnerType())) {
      return ownerInfo.getOwnerId() == userId;
    }
    if (!Entity.EntityType.GROUP.name().equalsIgnoreCase(ownerInfo.getOwnerType())) {
      return false;
    }
    EntityStore entityStore = GravitinoEnv.getInstance().entityStore();
    for (GroupEntity groupEntity : resolveCurrentUserGroups(metalake, entityStore)) {
      if (Objects.equals(groupEntity.id(), ownerInfo.getOwnerId())) {
        return true;
      }
    }
    return false;
  }

  // ---------------------------------------------------------------------------
  //  4-step role loading with version validation
  // ---------------------------------------------------------------------------

  private void loadRolePrivilege(
      String metalake,
      String username,
      long userId,
      UserUpdatedAt userInfo,
      AuthorizationRequestContext requestContext) {
    requestContext.loadRole(
        () -> {
          // Step 1a: version-validated user-direct roles via cache.
          List<Long> userDirectRoleIds = loadUserRoles(metalake, username, userId, userInfo);

          // Step 1b: version-validated group-inherited roles via cache. Group membership comes
          // from the IdP-pushed UserPrincipal; for each group we load its roles via the same
          // version-validated path as users (group_meta.updated_at as the staleness sentinel).
          List<Long> groupInheritedRoleIds = new ArrayList<>();
          for (String groupname : currentPrincipalGroupNames()) {
            groupInheritedRoleIds.addAll(
                loadGroupRoles(metalake, groupname, userId, requestContext));
          }

          // Prune stale g-rows: any role currently bound but no longer in the desired
          // set (e.g. user removed from a group at the IdP, or role unassigned).
          Set<String> desiredRoleIds = new HashSet<>();
          for (Long id : userDirectRoleIds) {
            desiredRoleIds.add(String.valueOf(id));
          }
          for (Long id : groupInheritedRoleIds) {
            desiredRoleIds.add(String.valueOf(id));
          }
          String userIdStr = String.valueOf(userId);
          for (String currentRole : allowEnforcer.getRolesForUser(userIdStr)) {
            if (!desiredRoleIds.contains(currentRole)) {
              allowEnforcer.deleteRoleForUser(userIdStr, currentRole);
              denyEnforcer.deleteRoleForUser(userIdStr, currentRole);
            }
          }

          // Step 3: batch version-check all role IDs (direct + group-inherited),
          // load stale ones (1 query for the version probe).
          List<Long> allRoleIds = new ArrayList<>(userDirectRoleIds);
          allRoleIds.addAll(groupInheritedRoleIds);
          if (!allRoleIds.isEmpty()) {
            versionCheckAndLoadRoles(metalake, allRoleIds, requestContext);
          }
        });
  }

  private List<Long> loadUserRoles(
      String metalake, String username, long userId, UserUpdatedAt userInfo) {
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

  /**
   * Per-request {@link GroupUpdatedAt} lookup, mirroring {@link #loadUserInfo}. The {@code
   * group_meta} probe runs at most once per (metalake, groupname) within a single request.
   */
  private Optional<GroupUpdatedAt> loadGroupInfo(
      String metalake, String groupname, AuthorizationRequestContext requestContext) {
    String cacheKey = metalake + KEY_SEP + groupname;
    return requestContext.computeGroupInfoIfAbsent(
        cacheKey,
        k ->
            Optional.ofNullable(
                SessionUtils.getWithoutCommit(
                    GroupMetaMapper.class, m -> m.getGroupUpdatedAt(metalake, groupname))));
  }

  /**
   * Version-validated group-role load, mirroring {@link #loadUserRoles}. Uses {@code
   * group_meta.updated_at} as the staleness sentinel: if the cached snapshot is at least as fresh
   * as the DB version, we reuse it; otherwise we reload from {@code role_meta}. In both cases the
   * resulting role IDs are bound to the user's jcasbin g-rows so that the enforcer sees inherited
   * privileges. Groups missing from the DB return an empty list.
   */
  private List<Long> loadGroupRoles(
      String metalake, String groupname, long userId, AuthorizationRequestContext requestContext) {
    Optional<GroupUpdatedAt> groupInfoOpt = loadGroupInfo(metalake, groupname, requestContext);
    if (!groupInfoOpt.isPresent()) {
      return new ArrayList<>();
    }
    GroupUpdatedAt groupInfo = groupInfoOpt.get();
    long groupId = groupInfo.getGroupId();
    String groupCacheKey = metalake + KEY_SEP + groupname;
    Optional<CachedGroupRoles> cachedOpt = groupRoleCache.getIfPresent(groupCacheKey);

    if (cachedOpt.isPresent() && cachedOpt.get().getUpdatedAt() >= groupInfo.getUpdatedAt()) {
      CachedGroupRoles cached = cachedOpt.get();
      bindUserRoles(userId, cached.getRoleIds());
      return cached.getRoleIds();
    }

    List<RolePO> rolePOs =
        SessionUtils.getWithoutCommit(RoleMetaMapper.class, m -> m.listRolesByGroupId(groupId));
    List<Long> roleIds = rolePOs.stream().map(RolePO::getRoleId).collect(Collectors.toList());

    groupRoleCache.put(
        groupCacheKey, new CachedGroupRoles(groupId, groupInfo.getUpdatedAt(), roleIds));
    bindUserRoles(userId, roleIds);
    return roleIds;
  }

  /**
   * Returns the current principal's group names as carried by the IdP-pushed {@link UserPrincipal}.
   * Returns an empty list when the principal is not a {@link UserPrincipal} (e.g. service tokens)
   * or has no groups.
   */
  private List<String> currentPrincipalGroupNames() {
    Principal principal = PrincipalUtils.getCurrentPrincipal();
    if (!(principal instanceof UserPrincipal)) {
      return new ArrayList<>();
    }
    List<UserGroup> groups = ((UserPrincipal) principal).getGroups();
    if (groups.isEmpty()) {
      return new ArrayList<>();
    }
    return groups.stream().map(UserGroup::getGroupname).collect(Collectors.toList());
  }

  /**
   * Resolves GroupEntity objects for the current principal's groups, skipping any that are stale or
   * not found in the store. Used by both {@link #isSelf} (ROLE branch) and {@link
   * #loadRolePrivilege} to discover group-inherited role assignments.
   */
  private List<GroupEntity> resolveCurrentUserGroups(String metalake, EntityStore entityStore) {
    Principal principal = PrincipalUtils.getCurrentPrincipal();
    if (!(principal instanceof UserPrincipal)) {
      return new ArrayList<>();
    }
    List<UserGroup> groups = ((UserPrincipal) principal).getGroups();
    if (groups.isEmpty()) {
      return new ArrayList<>();
    }
    List<NameIdentifier> groupIdents =
        groups.stream()
            .map(g -> NameIdentifierUtil.ofGroup(metalake, g.getGroupname()))
            .collect(Collectors.toList());
    return entityStore.batchGet(groupIdents, Entity.EntityType.GROUP, GroupEntity.class);
  }

  private void versionCheckAndLoadRoles(
      String metalake, List<Long> roleIds, AuthorizationRequestContext requestContext) {
    // Step 3: batch fetch (roleId, roleName, updated_at) for all role IDs — 1 query
    List<Long> uniqueRoleIds = roleIds.stream().distinct().collect(Collectors.toList());
    List<RoleUpdatedAt> roleVersions =
        SessionUtils.getWithoutCommit(
            RoleMetaMapper.class, m -> m.batchGetRoleUpdatedAt(uniqueRoleIds));

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
        loadPolicyByRoleEntity(roleEntity, requestContext);
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
  //  Policy loading from role entity
  // ---------------------------------------------------------------------------

  private void loadPolicyByRoleEntity(
      RoleEntity roleEntity, AuthorizationRequestContext requestContext) {
    String metalake = NameIdentifierUtil.getMetalake(roleEntity.nameIdentifier());
    List<SecurableObject> securableObjects = roleEntity.securableObjects();

    for (SecurableObject securableObject : securableObjects) {
      Long securableId = lookups.resolveMetadataId(securableObject, metalake, requestContext);
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
}
