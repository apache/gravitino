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

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
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
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.storage.relational.SupportsEntityChangeLog;
import org.apache.gravitino.storage.relational.mapper.GroupMetaMapper;
import org.apache.gravitino.storage.relational.mapper.RoleMetaMapper;
import org.apache.gravitino.storage.relational.mapper.UserMetaMapper;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.gravitino.storage.relational.po.auth.AuthPrefetchRow;
import org.apache.gravitino.storage.relational.po.auth.GroupUpdatedAt;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.po.auth.RoleUpdatedAt;
import org.apache.gravitino.storage.relational.po.auth.UserUpdatedAt;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The indexed implementation of {@link GravitinoAuthorizer}.
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
 *       correctness — TTL eviction only bounds memory. User/group role snapshots and loaded role
 *       policies both use write-based TTLs through {@link CaffeineGravitinoCache} and {@link
 *       JcasbinLoadedRolesCache}, respectively.
 *   <li><b>Eventual-consistency caches</b> — {@link #metadataIdCache} and {@link #ownerRelCache}.
 *       The global entity change log poller dispatches {@code entity_change_log} batches to {@link
 *       #changePoller}, while {@link #changePoller} polls {@code owner_meta}. Other Gravitino nodes
 *       therefore observe ALTER/DROP and owner changes within one poll interval.
 * </ol>
 *
 * <p>The pollers are best-effort and intentionally cheap; see {@link JcasbinChangeListener} for the
 * contracts they rely on (most notably that {@code entity_change_log.full_name} is the pre-mutation
 * name).
 *
 * <p>Role indexes are immutable. Each request merges them once into an effective allow/deny view,
 * so object checks do constant-time lookups without role graph traversal. Ordinary cache eviction
 * cannot remove a request's denies. Explicit invalidation and new policy publication refresh the
 * view before a new decision. Previously memoized decisions retain their request-scoped results.
 */
public class JcasbinAuthorizer implements GravitinoAuthorizer {

  private static final Logger LOG = LoggerFactory.getLogger(JcasbinAuthorizer.class);

  /**
   * How long to wait before retrying a role whose last policy load was incomplete, i.e. at least
   * one of its securable objects could not be resolved to a metadata id. Such a role is recorded
   * with its completeness flag in {@link #loadedRoles}; without this backoff every request carrying
   * the role would re-read the role entity and re-probe the missing object. A role can stay
   * unresolvable indefinitely — for example when it still references a dropped table — so the retry
   * has to be throttled rather than left to the version check, whose {@code role_meta.updated_at}
   * sentinel never moves in that case.
   */
  private static final long PARTIAL_ROLE_LOAD_RETRY_MS = 10_000L;

  /**
   * How many times a single authorization check reloads the caller's roles when their policies were
   * cleared after the request loaded them. Beyond this the check fails closed; see {@link
   * #evaluateWithLoadedRolePolicies}.
   */
  private static final int MAX_ROLE_POLICY_RELOADS = 3;

  /** Serializes publication and invalidation; database and metadata lookups stay outside it. */
  private final ReentrantReadWriteLock rolePolicyLock = new ReentrantReadWriteLock();

  /**
   * Source of role policy generations. Advanced under the write lock of {@link #rolePolicyLock}
   * whenever a role index is published or explicitly invalidated.
   */
  private final AtomicLong rolePolicyGenerationCounter = new AtomicLong();

  /**
   * roleId -> generation of its most recent publication or invalidation. An older request view must
   * refresh before evaluating new decisions for a changed role. Ordinary cache eviction does not
   * change policy generations. The map is bounded by {@link #maxRoleClearGenerations}; see {@link
   * #prunedRoleClearGeneration} for how pruned entries stay safe.
   */
  private final Map<Long, Long> roleClearGenerations = new ConcurrentHashMap<>();

  /**
   * Upper bound on {@link #roleClearGenerations} entries, set once to the role cache size in {@link
   * #initialize()} before use, and read under the write lock of {@link #rolePolicyLock}.
   */
  private long maxRoleClearGenerations;

  /**
   * The newest generation removed from {@link #roleClearGenerations} by pruning. A request whose
   * recorded generation is older can no longer prove that none of its roles was cleared, so it
   * reloads its roles once before the next check. Written under the write lock of {@link
   * #rolePolicyLock} and only ever increases.
   */
  private volatile long prunedRoleClearGeneration;

  /** In-flight loaders retain publication state even if the shared cache evicts the role. */
  private static final class RoleLoadState {
    private int readers;
    @Nullable private CachedRolePolicies policies;
    private long invalidatedAt;
  }

  private final Map<Long, RoleLoadState> loadingRoles = new HashMap<>();

  /** allow internal authorizer */
  private InternalAuthorizer allowInternalAuthorizer;

  /** deny internal authorizer */
  private InternalAuthorizer denyInternalAuthorizer;

  // ---- Version-validated caches (strong consistency) ----

  /**
   * userRoleCache: per-(metalake, userName) -> CachedUserRoleRels. Version-validated per request
   * via user_meta.updated_at.
   */
  private GravitinoCache<String, CachedUserRoleRels> userRoleCache;

  /**
   * groupRoleCache: per-(metalake, groupName) -> CachedGroupRoleRels. Version-validated per request
   * via group_meta.updated_at.
   */
  private GravitinoCache<String, CachedGroupRoleRels> groupRoleCache;

  /** roleId -> immutable policy index, version, completeness and denied-privilege summary. */
  private GravitinoCache<Long, CachedRolePolicies> loadedRoles;

  /**
   * Retry throttle for incomplete snapshots. It never represents policy completeness, and it cannot
   * suppress reloading an evicted index or a newer role version.
   */
  private GravitinoCache<Long, Boolean> partialRoleLoadBackoff;

  // ---- Eventual consistency caches (poller-driven) ----

  /** Path-based metadata object key -> entity id. Evicted by entity change poller. */
  private GravitinoCache<String, Long> metadataIdCache;

  /** ownerRelCache: metadataObjectId -> Optional(owner). Evicted by owner change poller. */
  private GravitinoCache<Long, Optional<OwnerInfo>> ownerRelCache;

  /** Two-tier lookup facade for metadata-id / owner (per-request dedup + Caffeine + DB). */
  private JcasbinAuthorizationLookups lookups;

  /** Background HA invalidator for {@link #metadataIdCache} and {@link #ownerRelCache}. */
  private JcasbinChangeListener changePoller;

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
    maxRoleClearGenerations = roleCacheSize;

    allowInternalAuthorizer = new InternalAuthorizer(true, false);
    denyInternalAuthorizer = new InternalAuthorizer(false, true);
    loadedRoles = new JcasbinLoadedRolesCache(ttlMs, roleCacheSize);
    partialRoleLoadBackoff =
        new CaffeineGravitinoCache<>(Math.min(PARTIAL_ROLE_LOAD_RETRY_MS, ttlMs), roleCacheSize);

    userRoleCache = new CaffeineGravitinoCache<>(ttlMs, roleCacheSize);
    groupRoleCache = new CaffeineGravitinoCache<>(ttlMs, roleCacheSize);
    // The change poller is the primary HA invalidation path. These write-based TTLs bound the
    // stale window if a poll cycle misses a change; access-based TTLs could keep hot stale entries
    // alive indefinitely.
    metadataIdCache = new CaffeineGravitinoCache<>(ttlMs, metadataIdCacheSize);
    ownerRelCache = new CaffeineGravitinoCache<>(ttlMs, ownerCacheSize);
    lookups = new JcasbinAuthorizationLookups(metadataIdCache, ownerRelCache);
    changePoller = new JcasbinChangeListener(metadataIdCache, ownerRelCache, pollIntervalSecs);
    EntityStore entityStore = GravitinoEnv.getInstance().entityStore();
    if (entityStore instanceof SupportsEntityChangeLog) {
      ((SupportsEntityChangeLog) entityStore).registerEntityChangeLogListener(changePoller);
    }
    changePoller.start();
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
    boolean result = false;
    // The metadataObject is resolved from an OGNL variable (e.g. SCHEMA, CATALOG) bound from the
    // request context when the authorization expression is evaluated. It can be null when the
    // expression references a metadata-object type that is not present for the current request,
    // so we treat a missing object as "not the owner".
    if (metadataObject == null) {
      return false;
    }

    if (metadataObject.type() == MetadataObject.Type.SCHEMA) {
      // We support hierarchical schema, so a schema may have ancestor schemas. The principal is
      // treated as the owner if it owns the schema itself or any of its ancestor schemas, hence we
      // walk the whole inheritance chain here.
      for (MetadataObject scopeObject : buildSchemaInheritanceChain(metadataObject)) {
        if (isOwnerOfObject(scopeObject, principal, metalake, requestContext)) {
          result = true;
          break;
        }
      }
    } else {
      result = isOwnerOfObject(metadataObject, principal, metalake, requestContext);
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

  /**
   * Resolves the owner of a single metadata object via the cache-backed lookups and checks whether
   * the given principal (directly or through one of its groups) is that owner. A missing object is
   * treated as "not the owner".
   */
  private boolean isOwnerOfObject(
      MetadataObject metadataObject,
      Principal principal,
      String metalake,
      AuthorizationRequestContext requestContext) {
    Optional<Long> metadataId = lookups.resolveMetadataId(metadataObject, metalake, requestContext);
    if (!metadataId.isPresent()) {
      return false;
    }
    Optional<OwnerInfo> owner =
        lookups.resolveOwnerId(metadataId.get(), metadataObject.type(), requestContext);
    return ownerMatchesUserOrGroups(owner, principal, metalake, requestContext);
  }

  @Override
  public boolean isServiceAdmin() {
    return GravitinoEnv.getInstance()
        .internalAccessControlDispatcher()
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
  public boolean hasDenyPolicy(
      Principal principal,
      String metalake,
      Set<Privilege.Name> privileges,
      AuthorizationRequestContext requestContext) {
    try {
      Optional<UserUpdatedAt> userInfoOpt =
          loadUserInfo(metalake, principal.getName(), requestContext);
      if (!userInfoOpt.isPresent()) {
        // An unknown user holds no roles and therefore no deny policies.
        return false;
      }
      UserUpdatedAt userInfo = userInfoOpt.get();
      long userId = userInfo.getUserId();
      // Resolve the direct and current IdP-group roles into a request-local policy view.
      loadRolePrivilege(metalake, principal.getName(), userId, userInfo, requestContext);

      Set<String> privilegeNames = privileges.stream().map(Enum::name).collect(Collectors.toSet());
      return evaluateWithLoadedRolePolicies(
              metalake, requestContext, view -> view.hasDeny(privilegeNames))
          .orElse(true);
    } catch (RuntimeException e) {
      LOG.warn(
          "Cannot establish deny policies for user {} in {}; disabling list shortcut",
          principal.getName(),
          metalake,
          e);
      return true;
    }
  }

  @Override
  public boolean isSelf(
      Entity.EntityType type,
      NameIdentifier nameIdentifier,
      AuthorizationRequestContext requestContext) {
    String metalake = nameIdentifier.namespace().level(0);
    if (Entity.EntityType.USER == type) {
      String currentUserName = PrincipalUtils.getCurrentUserName();
      return Objects.equals(nameIdentifier.name(), currentUserName);
    } else if (Entity.EntityType.GROUP == type) {
      Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
      if (!(currentPrincipal instanceof UserPrincipal)) {
        return false;
      }

      List<UserGroup> groups = ((UserPrincipal) currentPrincipal).getGroups();
      if (groups.isEmpty()) {
        return false;
      }

      boolean principalHasGroup =
          groups.stream()
              .map(UserGroup::getGroupName)
              .anyMatch(groupName -> Objects.equals(groupName, nameIdentifier.name()));
      return principalHasGroup;
    } else if (Entity.EntityType.ROLE == type) {
      String currentUserName = PrincipalUtils.getCurrentUserName();
      try {
        Optional<Long> roleId =
            MetadataIdConverter.getID(
                NameIdentifierUtil.toMetadataObject(nameIdentifier, type), metalake);
        if (!roleId.isPresent()) {
          return false;
        }
        long resolvedRoleId = roleId.get();

        Optional<UserUpdatedAt> userInfoOpt =
            loadUserInfo(metalake, currentUserName, requestContext);
        if (!userInfoOpt.isPresent()) {
          return false;
        }
        UserUpdatedAt userInfo = userInfoOpt.get();
        long userId = userInfo.getUserId();

        List<Long> directRoleIds = loadUserRoles(metalake, currentUserName, userId, userInfo);
        if (directRoleIds.contains(resolvedRoleId)) {
          return true;
        }

        for (String groupname : currentPrincipalGroupNames()) {
          List<Long> groupRoleIds = loadGroupRoles(metalake, groupname, requestContext);
          if (groupRoleIds.contains(resolvedRoleId)) {
            return true;
          }
        }
        return false;

      } catch (Exception e) {
        LOG.warn("Cannot get user ID or role ID", e);
        return false;
      }
    }
    throw new UnsupportedOperationException("Unsupported Entity Type.");
  }

  @Override
  public Set<String> findUnheldRoles(
      Principal principal,
      String metalake,
      Set<String> declaredRoleNames,
      AuthorizationRequestContext requestContext) {
    if (declaredRoleNames == null || declaredRoleNames.isEmpty()) {
      return new LinkedHashSet<>();
    }
    String username = principal.getName();
    Set<String> heldRoleNames;
    try {
      List<String> groupNames = principalGroupNames(principal);
      // Prime the role caches and versions that the downstream authorize() call will reuse.
      Optional<UserUpdatedAt> userInfoOpt =
          prefetchUserAndGroupInfo(metalake, username, groupNames, requestContext);
      if (!userInfoOpt.isPresent()) {
        // No user record => the caller holds no roles, so every declared role is unheld.
        return new LinkedHashSet<>(declaredRoleNames);
      }
      Map<Long, RoleUpdatedAt> roleVersions = requestContext.getPrefetchedRoleVersions();
      heldRoleNames =
          roleVersions.values().stream()
              .map(RoleUpdatedAt::getRoleName)
              .collect(Collectors.toSet());
    } catch (Exception e) {
      // Fail closed: if membership cannot be resolved, treat every declared role as unheld.
      LOG.warn(
          "Failed to resolve held roles for user {} in metalake {}; rejecting role assumption",
          username,
          metalake,
          e);
      return new LinkedHashSet<>(declaredRoleNames);
    }

    Set<String> unheldRoles = new LinkedHashSet<>();
    for (String roleName : declaredRoleNames) {
      if (!heldRoleNames.contains(roleName)) {
        unheldRoles.add(roleName);
      }
    }
    return unheldRoles;
  }

  @Override
  public boolean hasSetOwnerPermission(
      String metalake, String type, String fullName, AuthorizationRequestContext requestContext) {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    MetadataObject.Type metadataType = MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT));
    MetadataObject metalakeObject =
        MetadataObjects.of(ImmutableList.of(metalake), MetadataObject.Type.METALAKE);

    // metalake owner can set owner in metalake.
    if (isOwner(currentPrincipal, metalake, metalakeObject, requestContext)) {
      return true;
    }

    MetadataObject metadataObject = MetadataObjects.parse(fullName, metadataType);
    do {
      if (isOwner(currentPrincipal, metalake, metadataObject, requestContext)
          && hasParentUsagePermission(
              currentPrincipal, metalake, metadataObject, metalakeObject, requestContext)) {
        return true;
      }
    } while ((metadataObject = MetadataObjects.parent(metadataObject)) != null);
    return false;
  }

  private boolean hasAuthorizeWithoutDeny(
      Principal principal,
      String metalake,
      List<MetadataObject> authorizeObjects,
      List<MetadataObject> denyObjects,
      Privilege.Name privilege,
      AuthorizationRequestContext requestContext) {
    return hasAuthorizeOnAny(principal, metalake, authorizeObjects, privilege, requestContext)
        && !hasDenyOnAny(principal, metalake, denyObjects, privilege, requestContext);
  }

  private boolean hasAuthorizeOnAny(
      Principal principal,
      String metalake,
      List<MetadataObject> metadataObjects,
      Privilege.Name privilege,
      AuthorizationRequestContext requestContext) {
    for (MetadataObject metadataObject : metadataObjects) {
      if (authorize(principal, metalake, metadataObject, privilege, requestContext)) {
        return true;
      }
    }
    return false;
  }

  private boolean hasDenyOnAny(
      Principal principal,
      String metalake,
      List<MetadataObject> metadataObjects,
      Privilege.Name privilege,
      AuthorizationRequestContext requestContext) {
    for (MetadataObject metadataObject : metadataObjects) {
      if (deny(principal, metalake, metadataObject, privilege, requestContext)) {
        return true;
      }
    }
    return false;
  }

  private boolean hasParentUsagePermission(
      Principal principal,
      String metalake,
      MetadataObject targetObject,
      MetadataObject metalakeObject,
      AuthorizationRequestContext requestContext) {
    MetadataObject parentObject = MetadataObjects.parent(targetObject);
    if (parentObject != null && parentObject.type() == MetadataObject.Type.CATALOG) {
      List<MetadataObject> useCatalogObjects = ImmutableList.of(parentObject, metalakeObject);
      return hasAuthorizeWithoutDeny(
          principal,
          metalake,
          useCatalogObjects,
          useCatalogObjects,
          Privilege.Name.USE_CATALOG,
          requestContext);
    }
    if (parentObject != null && parentObject.type() == MetadataObject.Type.SCHEMA) {
      MetadataObject catalogObject = MetadataObjects.parent(parentObject);
      List<MetadataObject> useSchemaObjects =
          ImmutableList.of(metalakeObject, catalogObject, parentObject);
      return hasAuthorizeWithoutDeny(
              principal,
              metalake,
              useSchemaObjects,
              useSchemaObjects,
              Privilege.Name.USE_SCHEMA,
              requestContext)
          && !hasDenyOnAny(
              principal,
              metalake,
              ImmutableList.of(catalogObject, metalakeObject),
              Privilege.Name.USE_CATALOG,
              requestContext);
    }
    return true;
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
    MetadataObject targetObject = MetadataObjects.parse(fullName, metadataType);
    MetadataObject metalakeObject =
        MetadataObjects.of(ImmutableList.of(metalake), MetadataObject.Type.METALAKE);
    List<MetadataObject> chain = new ArrayList<>();
    for (MetadataObject obj = targetObject; obj != null; obj = MetadataObjects.parent(obj)) {
      chain.add(obj);
    }
    chain.add(metalakeObject);

    if (hasAuthorizeWithoutDeny(
            currentPrincipal, metalake, chain, chain, Privilege.Name.MANAGE_GRANTS, requestContext)
        && hasParentUsagePermission(
            currentPrincipal, metalake, targetObject, metalakeObject, requestContext)) {
      return true;
    }
    return hasSetOwnerPermission(metalake, type, fullName, requestContext);
  }

  // ---------------------------------------------------------------------------
  //  Cache invalidation hooks (called from service layer)
  // ---------------------------------------------------------------------------

  @Override
  public void handleRolePrivilegeChange(Long roleId) {
    invalidateRolePolicies(roleId, true);
  }

  @Override
  public void handleUserRoleRelChange(String metalake, String userName) {
    userRoleCache.invalidate(JcasbinAuthorizationCacheKeys.userRoleKey(metalake, userName));
  }

  @Override
  public void handleGroupRoleRelChange(String metalake, String groupName) {
    groupRoleCache.invalidate(JcasbinAuthorizationCacheKeys.groupRoleKey(metalake, groupName));
  }

  @Override
  public void handleMetadataOwnerChange(
      String metalake, Long oldOwnerId, NameIdentifier nameIdentifier, Entity.EntityType type) {
    MetadataObject metadataObject = NameIdentifierUtil.toMetadataObject(nameIdentifier, type);
    // Owner mutations may happen after drop/recreate with the same name. Invalidate the
    // name->id mapping as well to prevent using a stale metadataId from metadataIdCache.
    metadataIdCache.invalidate(
        JcasbinAuthorizationCacheKeys.metadataIdCacheKey(metalake, metadataObject));
    try {
      MetadataIdConverter.getID(metadataObject, metalake).ifPresent(ownerRelCache::invalidate);
    } catch (RuntimeException e) {
      LOG.warn("Failed to resolve metadata id for owner cache invalidation: {}", metadataObject, e);
    }
  }

  @Override
  public void handleEntityNameIdMappingChange(
      String metalake, NameIdentifier nameIdentifier, Entity.EntityType type) {
    MetadataObject metadataObject = NameIdentifierUtil.toMetadataObject(nameIdentifier, type);
    String cacheKey = JcasbinAuthorizationCacheKeys.metadataIdCacheKey(metalake, metadataObject);
    if (JcasbinAuthorizationCacheKeys.hasNestedMetadataObjects(metadataObject.type())) {
      // Prefix invalidation removes the object and all nested objects under the same name path.
      metadataIdCache.invalidateByPrefix(cacheKey);
    } else {
      metadataIdCache.invalidate(cacheKey);
    }
  }

  @Override
  public void close() throws IOException {
    if (changePoller != null) {
      EntityStore entityStore = GravitinoEnv.getInstance().entityStore();
      if (entityStore instanceof SupportsEntityChangeLog) {
        ((SupportsEntityChangeLog) entityStore).unregisterEntityChangeLogListener(changePoller);
      }
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
    if (partialRoleLoadBackoff != null) {
      partialRoleLoadBackoff.close();
    }
    if (metadataIdCache != null) {
      metadataIdCache.close();
    }
    if (ownerRelCache != null) {
      ownerRelCache.close();
    }
  }

  /**
   * Builds the logical schema inheritance chain for a SCHEMA MetadataObject, ordered from the
   * outermost ancestor to the schema itself. For a schema object whose parent is {@code catalog}
   * and whose name is {@code "A:B:C"}, this returns MetadataObjects for parent {@code catalog} with
   * schema names {@code A}, {@code A:B}, and {@code A:B:C} in that order so that an ancestor-level
   * privilege grant short-circuits the authorization check before descending into more specific
   * scopes.
   *
   * <p>For flat (non-HierarchicalSchema) schemas the list contains only the original object.
   */
  private List<MetadataObject> buildSchemaInheritanceChain(MetadataObject schemaObject) {
    String separator = HierarchicalSchemaUtil.schemaSeparator();

    List<String> scopes = HierarchicalSchemaUtil.allScopes(schemaObject.name(), separator);
    List<MetadataObject> chain = new ArrayList<>(scopes.size());
    for (int i = scopes.size() - 1; i >= 0; i--) {
      chain.add(
          MetadataObjects.of(schemaObject.parent(), scopes.get(i), MetadataObject.Type.SCHEMA));
    }
    return ImmutableList.copyOf(chain);
  }

  // ---------------------------------------------------------------------------
  //  Internal authorizer
  // ---------------------------------------------------------------------------

  private class InternalAuthorizer {

    /**
     * When {@code true}, evaluation considers only the request's active roles instead of every role
     * the caller holds. Enabled for the allow authorizer so role assumption can drop allows; the
     * deny authorizer leaves it {@code false} so denies always apply.
     */
    private final boolean narrowByActiveRoles;

    /**
     * The fail-closed result returned when the caller's role policies cannot be kept loaded for the
     * duration of a check, or when the checked object's name cannot be normalized: {@code false}
     * for the allow authorizer and {@code true} for the deny authorizer, so an unstable policy
     * state can only reject a request.
     */
    private final boolean unstablePolicyResult;

    public InternalAuthorizer(boolean narrowByActiveRoles, boolean unstablePolicyResult) {
      this.narrowByActiveRoles = narrowByActiveRoles;
      this.unstablePolicyResult = unstablePolicyResult;
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
      // OWNER uses the owner cache independently of indexed role privileges. Skip the fat
      // prefetch and role loading when no non-OWNER
      // privilege has been evaluated yet in this request.
      boolean ownerOnly =
          AuthConstants.OWNER.equals(privilege)
              && requestContext.getPrefetchedRoleVersions() == null;

      long userId;
      UserUpdatedAt userInfo;
      try {
        Optional<UserUpdatedAt> userInfoOpt;
        if (ownerOnly || requestContext.getPrefetchedRoleVersions() != null) {
          userInfoOpt = loadUserInfo(metalake, username, requestContext);
        } else {
          userInfoOpt =
              prefetchUserAndGroupInfo(
                  metalake, username, currentPrincipalGroupNames(), requestContext);
        }
        if (!userInfoOpt.isPresent()) {
          LOG.debug("User {} not found in metalake {}", username, metalake);
          return false;
        }
        userInfo = userInfoOpt.get();
        userId = userInfo.getUserId();
      } catch (Exception e) {
        LOG.warn("Cannot read authorization subject {} in {}", username, metalake, e);
        return unstablePolicyResult;
      }

      if (!ownerOnly) {
        // Version-validate role indexes for non-OWNER checks.
        loadRolePrivilege(metalake, username, userId, userInfo, requestContext);
      }

      // For requests such as CREATE SCHEMA, the metadata object may be null. This method
      // performs object-scoped authorization, so without a metadata object it cannot evaluate
      // the request and must deny authorization here.
      if (metadataObject == null) {
        return false;
      }
      // For SCHEMA objects with hierarchical schema names (for example, parent=catalog and
      // name="A:B:C"), walk the logical parent chain from the outermost ancestor down to the
      // schema itself so that a privilege granted on an ancestor schema short-circuits the check
      // before descending into more specific scopes.
      if (metadataObject.type() == MetadataObject.Type.SCHEMA) {
        for (MetadataObject scopeObject : buildSchemaInheritanceChain(metadataObject)) {
          if (authorizeObject(userId, metalake, scopeObject, privilege, requestContext)) {
            return true;
          }
        }
        return false;
      }

      return authorizeObject(userId, metalake, metadataObject, privilege, requestContext);
    }

    /**
     * Resolves the metadata id for a single object via the cache-backed lookups and delegates to
     * {@link #authorizeByPolicyIndex}. A missing object is treated as "not authorized".
     */
    private boolean authorizeObject(
        long userId,
        String metalake,
        MetadataObject metadataObject,
        String privilege,
        AuthorizationRequestContext requestContext) {
      JcasbinAuthorizationLookups.MetadataIdResolution resolution =
          lookups.resolveMetadataIdResult(metadataObject, metalake, requestContext);
      if (resolution.normalizationFailed()) {
        // Failure to resolve a DENY scope must not let a parent ALLOW grant access, even when
        // the role's policies are already loaded. Missing entities retain their existing semantics.
        return unstablePolicyResult;
      }
      Optional<Long> metadataId = resolution.metadataId();
      if (!metadataId.isPresent()) {
        return false;
      }
      return authorizeByPolicyIndex(
          userId, metalake, metadataObject, metadataId.get(), privilege, requestContext);
    }

    private boolean authorizeByPolicyIndex(
        long userId,
        String metalake,
        MetadataObject metadataObject,
        Long metadataId,
        String privilege,
        AuthorizationRequestContext requestContext) {
      // Step 4: Indexed policy lookup (pure in-memory) — except OWNER, which is resolved via the
      // owner cache rather than g-rows.
      if (AuthConstants.OWNER.equals(privilege)) {
        // Cold-path: resolveOwnerId loads from DB when neither the per-request nor the shared
        // Caffeine cache has the entry, ensuring the first OWNER check doesn't spuriously deny.
        Optional<OwnerInfo> owner =
            lookups.resolveOwnerId(metadataId, metadataObject.type(), requestContext);
        return ownerMatchesUserOrGroups(
            owner, PrincipalUtils.getCurrentPrincipal(), metalake, requestContext);
      }

      PolicyKey key = new PolicyKey(metadataObject.type().name(), metadataId, privilege);
      boolean result =
          evaluateWithLoadedRolePolicies(
                  metalake, requestContext, view -> view.evaluate(key, narrowByActiveRoles))
              .orElse(unstablePolicyResult);
      LOG.debug("Indexed privilege check for user {} on {}: {}", userId, key, result);
      return result;
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
    String cacheKey = JcasbinAuthorizationCacheKeys.userRoleKey(metalake, username);
    return requestContext.computeUserInfoIfAbsent(
        cacheKey,
        k ->
            Optional.ofNullable(
                SessionUtils.getWithoutCommit(
                    UserMetaMapper.class, m -> m.getUserUpdatedAt(metalake, username))));
  }

  /**
   * Fat-JOIN prefetch: collapses {@link #loadUserInfo}, per-group {@link #loadGroupInfo}, the
   * per-user/per-group role-list lookups inside {@link #loadUserRoles} / {@link #loadGroupRoles},
   * AND the role-version probe inside {@link #loadRequestPolicies} into a single SQL round trip.
   * After this returns, the following caches are primed and the rest of the authorize hot path
   * needs zero DB round trips when the cached role policies are still current:
   *
   * <ul>
   *   <li>{@code requestContext.userInfoCache} — user version sentinel.
   *   <li>{@code requestContext.groupInfoCache} — per-group version sentinel; absent groups are
   *       negative-cached so callers can short-circuit.
   *   <li>{@code userRoleCache} (process-wide) — refreshed with the user's current direct role ids
   *       at the just-read user version, so the next {@link #loadUserRoles} call observes a
   *       version-validated cache hit.
   *   <li>{@code groupRoleCache} (process-wide) — same idea per group.
   *   <li>{@code requestContext.prefetchedRoleVersions} — roleId → {@link RoleUpdatedAt} map
   *       consumed by {@link #loadRequestPolicies} to skip its dedicated probe.
   * </ul>
   *
   * <p>The fat prefetch runs at most once per request, gated by {@code prefetchedRoleVersions}.
   */
  private Optional<UserUpdatedAt> prefetchUserAndGroupInfo(
      String metalake,
      String username,
      List<String> groupNames,
      AuthorizationRequestContext requestContext) {

    String userKey = JcasbinAuthorizationCacheKeys.userRoleKey(metalake, username);
    if (requestContext.getPrefetchedRoleVersions() != null) {
      return loadUserInfo(metalake, username, requestContext);
    }

    // Single round-trip pulls the request user, its groups, and both direct + inherited role
    // bindings as one flat polymorphic list. See AuthPrefetchRow for the per-Kind field layout.
    List<AuthPrefetchRow> rows =
        SessionUtils.getWithoutCommit(
            UserMetaMapper.class,
            m -> m.batchGetAuthSubjectsForUser(metalake, username, groupNames));

    UserUpdatedAt foundUser = null;
    Map<String, GroupUpdatedAt> foundGroups = new HashMap<>();
    Map<Long, RoleUpdatedAt> roleVersions = new HashMap<>();
    LinkedHashSet<Long> userRoleIds = new LinkedHashSet<>();
    Map<Long, LinkedHashSet<Long>> groupRoleIdsByGroupId = new HashMap<>();

    // Pivot the flat row list into per-Kind buckets. Each branch reads exactly the fields the
    // class-level Javadoc of AuthPrefetchRow documents as meaningful for that Kind.
    for (AuthPrefetchRow row : rows) {
      switch (row.getSubjectType()) {
        case USER:
          // entityId = user_id, updatedAt = user_meta.updated_at. At most one row.
          foundUser = new UserUpdatedAt(row.getEntityId(), row.getUpdatedAt());
          break;
        case GROUP:
          // entityId = group_id, entityName = group_name, updatedAt = group_meta.updated_at.
          foundGroups.put(
              row.getEntityName(), new GroupUpdatedAt(row.getEntityId(), row.getUpdatedAt()));
          break;
        case USER_ROLE:
          // entityId = role_id, entityName = role_name, updatedAt = role_meta.updated_at.
          // bindingOwnerId is the user this role is bound to; not needed here because the user is
          // implicit (we already know `username`).
          userRoleIds.add(row.getEntityId());
          roleVersions.put(
              row.getEntityId(),
              new RoleUpdatedAt(row.getEntityId(), row.getEntityName(), row.getUpdatedAt()));
          break;
        case GROUP_ROLE:
          // entityId = role_id, entityName = role_name, updatedAt = role_meta.updated_at.
          // bindingOwnerId = owning group_id — used to bucket roles back to their group.
          Long parentGroupId = row.getBindingOwnerId();
          if (parentGroupId != null) {
            groupRoleIdsByGroupId
                .computeIfAbsent(parentGroupId, p -> new LinkedHashSet<>())
                .add(row.getEntityId());
          }
          roleVersions.put(
              row.getEntityId(),
              new RoleUpdatedAt(row.getEntityId(), row.getEntityName(), row.getUpdatedAt()));
          break;
        default:
          break;
      }
    }

    Optional<UserUpdatedAt> foundUserOpt = Optional.ofNullable(foundUser);
    requestContext.computeUserInfoIfAbsent(userKey, k -> foundUserOpt);

    for (String groupName : groupNames) {
      String groupKey = JcasbinAuthorizationCacheKeys.groupRoleKey(metalake, groupName);
      final Optional<GroupUpdatedAt> groupValue = Optional.ofNullable(foundGroups.get(groupName));
      requestContext.computeGroupInfoIfAbsent(groupKey, gk -> groupValue);
    }

    if (foundUser != null) {
      userRoleCache.put(
          JcasbinAuthorizationCacheKeys.userRoleKey(metalake, username),
          new CachedUserRoleRels(
              foundUser.getUserId(), foundUser.getUpdatedAt(), new ArrayList<>(userRoleIds)));
    }

    for (Map.Entry<String, GroupUpdatedAt> e : foundGroups.entrySet()) {
      String gname = e.getKey();
      GroupUpdatedAt ginfo = e.getValue();
      LinkedHashSet<Long> ridSet =
          groupRoleIdsByGroupId.getOrDefault(ginfo.getGroupId(), new LinkedHashSet<>());
      groupRoleCache.put(
          JcasbinAuthorizationCacheKeys.groupRoleKey(metalake, gname),
          new CachedGroupRoleRels(
              ginfo.getGroupId(), ginfo.getUpdatedAt(), new ArrayList<>(ridSet)));
    }

    requestContext.setPrefetchedRoleVersions(roleVersions);

    return foundUserOpt;
  }

  /**
   * Returns true when the cached owner type and ID match the given principal or one of the
   * principal's groups. The user id is resolved via the version-validated {@link #loadUserInfo}
   * cache so back-to-back ownership checks in the same request do not re-query {@code user_meta}.
   * Group ids are resolved via {@link #loadGroupInfo}, which deduplicates within the request via
   * {@code requestContext.groupInfoCache} and avoids loading full group entity objects.
   */
  private boolean ownerMatchesUserOrGroups(
      Optional<OwnerInfo> owner,
      Principal principal,
      String metalake,
      AuthorizationRequestContext requestContext) {
    if (!owner.isPresent()) {
      return false;
    }
    OwnerInfo ownerInfo = owner.get();
    if (Entity.EntityType.USER.name().equalsIgnoreCase(ownerInfo.getOwnerType())) {
      Optional<UserUpdatedAt> userInfo =
          loadUserInfo(metalake, principal.getName(), requestContext);
      return userInfo.isPresent() && userInfo.get().getUserId() == ownerInfo.getOwnerId();
    }
    if (!Entity.EntityType.GROUP.name().equalsIgnoreCase(ownerInfo.getOwnerType())) {
      return false;
    }
    for (String groupName : principalGroupNames(principal)) {
      Optional<GroupUpdatedAt> groupInfo = loadGroupInfo(metalake, groupName, requestContext);
      if (groupInfo.isPresent() && groupInfo.get().getGroupId() == ownerInfo.getOwnerId()) {
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
          // Read the generation before any role is loaded, so a clear that races with this load is
          // newer than the recorded generation and is caught by the first check that follows.
          long rolePolicyGeneration = rolePolicyGenerationCounter.get();

          // Step 1a: version-validated user-direct roles via cache.
          List<Long> userDirectRoleIds = loadUserRoles(metalake, username, userId, userInfo);

          // Step 1b: version-validated group-inherited roles via cache. Group membership comes
          // from the IdP-pushed UserPrincipal; for each group we load its roles via the same
          // version-validated path as users (group_meta.updated_at as the staleness sentinel).
          List<Long> groupInheritedRoleIds = new ArrayList<>();
          for (String groupname : currentPrincipalGroupNames()) {
            groupInheritedRoleIds.addAll(loadGroupRoles(metalake, groupname, requestContext));
          }

          Set<Long> allRoleIds = new LinkedHashSet<>(userDirectRoleIds);
          allRoleIds.addAll(groupInheritedRoleIds);
          requestContext.setBoundRoleIds(new ArrayList<>(allRoleIds));
          loadRequestPolicies(metalake, requestContext, rolePolicyGeneration);
        });
  }

  private List<Long> loadUserRoles(
      String metalake, String username, long userId, UserUpdatedAt userInfo) {
    String userCacheKey = JcasbinAuthorizationCacheKeys.userRoleKey(metalake, username);
    Optional<CachedUserRoleRels> cachedOpt = userRoleCache.getIfPresent(userCacheKey);

    if (cachedOpt.isPresent()
        && cachedOpt.get().getUserId() == userId
        && cachedOpt.get().getUpdatedAt() >= userInfo.getUpdatedAt()) {
      // Cache is still valid. The user id check prevents reusing roles after deleting and
      // recreating the same username with a new entity id.
      CachedUserRoleRels cached = cachedOpt.get();
      return cached.getRoleIds();
    }

    // Cache miss or stale — reload from DB
    List<RolePO> rolePOs =
        SessionUtils.getWithoutCommit(RoleMetaMapper.class, m -> m.listRolesByUserId(userId));
    List<Long> roleIds = rolePOs.stream().map(RolePO::getRoleId).collect(Collectors.toList());

    userRoleCache.put(
        userCacheKey, new CachedUserRoleRels(userId, userInfo.getUpdatedAt(), roleIds));
    return roleIds;
  }

  /**
   * Per-request {@link GroupUpdatedAt} lookup, mirroring {@link #loadUserInfo}. The {@code
   * group_meta} probe runs at most once per (metalake, groupname) within a single request.
   */
  private Optional<GroupUpdatedAt> loadGroupInfo(
      String metalake, String groupname, AuthorizationRequestContext requestContext) {
    String cacheKey = JcasbinAuthorizationCacheKeys.groupRoleKey(metalake, groupname);
    return requestContext.computeGroupInfoIfAbsent(
        cacheKey,
        k ->
            Optional.ofNullable(
                SessionUtils.getWithoutCommit(
                    GroupMetaMapper.class, m -> m.getGroupUpdatedAt(metalake, groupname))));
  }

  /**
   * Version-validated group-role load, mirroring {@link #loadUserRoles}. A cached snapshot is valid
   * only when it belongs to the current group id and is at least as fresh as {@code
   * group_meta.updated_at}; the group id check prevents reusing stale roles after a
   * delete-and-create of the same group name. The resulting IDs belong only to the current request;
   * no shared user/group role graph is mutated. Missing groups return an empty list.
   */
  private List<Long> loadGroupRoles(
      String metalake, String groupname, AuthorizationRequestContext requestContext) {
    Optional<GroupUpdatedAt> groupInfoOpt = loadGroupInfo(metalake, groupname, requestContext);
    if (!groupInfoOpt.isPresent()) {
      return new ArrayList<>();
    }
    GroupUpdatedAt groupInfo = groupInfoOpt.get();
    long groupId = groupInfo.getGroupId();
    String groupCacheKey = JcasbinAuthorizationCacheKeys.groupRoleKey(metalake, groupname);
    Optional<CachedGroupRoleRels> cachedOpt = groupRoleCache.getIfPresent(groupCacheKey);

    if (cachedOpt.isPresent()) {
      CachedGroupRoleRels cached = cachedOpt.get();
      if (cached.getGroupId() == groupId && cached.getUpdatedAt() >= groupInfo.getUpdatedAt()) {
        return cached.getRoleIds();
      }
    }

    List<RolePO> rolePOs =
        SessionUtils.getWithoutCommit(RoleMetaMapper.class, m -> m.listRolesByGroupId(groupId));
    List<Long> roleIds = rolePOs.stream().map(RolePO::getRoleId).collect(Collectors.toList());

    groupRoleCache.put(
        groupCacheKey, new CachedGroupRoleRels(groupId, groupInfo.getUpdatedAt(), roleIds));
    return roleIds;
  }

  /**
   * Returns the current principal's group names as carried by the IdP-pushed {@link UserPrincipal}.
   * Returns an empty list when the principal is not a {@link UserPrincipal} (e.g. service tokens)
   * or has no groups.
   */
  private List<String> currentPrincipalGroupNames() {
    return principalGroupNames(PrincipalUtils.getCurrentPrincipal());
  }

  private List<String> principalGroupNames(Principal principal) {
    if (!(principal instanceof UserPrincipal)) {
      return new ArrayList<>();
    }
    List<UserGroup> groups = ((UserPrincipal) principal).getGroups();
    if (groups.isEmpty()) {
      return new ArrayList<>();
    }
    return groups.stream().map(UserGroup::getGroupName).collect(Collectors.toList());
  }

  private void loadRequestPolicies(
      String metalake, AuthorizationRequestContext context, long generation) {
    List<Long> ids = context.getBoundRoleIds().stream().distinct().collect(Collectors.toList());
    Map<Long, RoleLoadState> states = new HashMap<>();
    rolePolicyLock.writeLock().lock();
    try {
      for (long id : ids) {
        RoleLoadState state = loadingRoles.computeIfAbsent(id, key -> new RoleLoadState());
        state.readers++;
        CachedRolePolicies cached = loadedRoles.getIfPresent(id).orElse(null);
        if (cached != null
            && (state.policies == null
                || cached.getUpdatedAt() > state.policies.getUpdatedAt()
                || (cached.getUpdatedAt() == state.policies.getUpdatedAt()
                    && cached.isComplete()))) {
          state.policies = cached;
        }
        states.put(id, state);
      }
    } finally {
      rolePolicyLock.writeLock().unlock();
    }
    Map<Long, CachedRolePolicies> policies = new HashMap<>();
    Set<Long> unreadable = new HashSet<>();
    try {
      Map<Long, RoleUpdatedAt> versions = new HashMap<>();
      Map<Long, RoleUpdatedAt> prefetched =
          context.getRolePolicyView() == null ? context.getPrefetchedRoleVersions() : null;
      List<Long> missing = new ArrayList<>();
      for (long id : ids) {
        RoleUpdatedAt version = prefetched == null ? null : prefetched.get(id);
        if (version == null) {
          missing.add(id);
        } else {
          versions.put(id, version);
        }
      }
      if (!missing.isEmpty()) {
        for (RoleUpdatedAt version :
            SessionUtils.getWithoutCommit(
                RoleMetaMapper.class, mapper -> mapper.batchGetRoleUpdatedAt(missing))) {
          versions.put(version.getRoleId(), version);
        }
      }
      List<RoleUpdatedAt> stale = new ArrayList<>();
      for (long id : ids) {
        RoleUpdatedAt version = versions.get(id);
        if (version == null) {
          invalidateRolePolicies(id);
          continue;
        }
        rolePolicyLock.readLock().lock();
        try {
          CachedRolePolicies cached = states.get(id).policies;
          if (cached != null
              && cached.getUpdatedAt() >= version.getUpdatedAt()
              && (cached.isComplete() || partialRoleLoadBackoff.getIfPresent(id).isPresent())) {
            policies.put(id, cached);
          } else {
            stale.add(version);
          }
        } finally {
          rolePolicyLock.readLock().unlock();
        }
      }
      if (!stale.isEmpty()) {
        EntityStore store = GravitinoEnv.getInstance().entityStore();
        List<NameIdentifier> names =
            stale.stream()
                .map(v -> NameIdentifierUtil.ofRole(metalake, v.getRoleName()))
                .collect(Collectors.toList());
        List<RoleEntity> entities;
        try {
          entities = store.batchGet(names, Entity.EntityType.ROLE, RoleEntity.class);
        } catch (Exception e) {
          LOG.warn("Failed to batch load role policies; retrying individually", e);
          entities = Collections.emptyList();
        }
        Map<Long, RoleEntity> byId = new HashMap<>();
        if (entities != null) {
          for (RoleEntity entity : entities) {
            if (entity != null) {
              byId.put(entity.id(), entity);
            }
          }
        }
        for (RoleUpdatedAt version : stale) {
          long id = version.getRoleId();
          RoleEntity entity = byId.get(id);
          if (entity == null) {
            try {
              entity =
                  store.get(
                      NameIdentifierUtil.ofRole(metalake, version.getRoleName()),
                      Entity.EntityType.ROLE,
                      RoleEntity.class);
              if (entity == null || entity.id() != id) {
                unreadable.add(id);
                continue;
              }
            } catch (NoSuchEntityException e) {
              invalidateRolePolicies(id);
              continue;
            } catch (Exception e) {
              LOG.warn("Failed to read role {}", id, e);
              unreadable.add(id);
              continue;
            }
          }
          Optional<ResolvedRolePolicies> resolution = resolveRolePolicies(entity, context);
          if (!resolution.isPresent()) {
            unreadable.add(id);
            continue;
          }
          ResolvedRolePolicies resolved = resolution.get();
          CachedRolePolicies candidate =
              new CachedRolePolicies(
                  version.getUpdatedAt(),
                  resolved.getIndex(),
                  resolved.isComplete(),
                  resolved.getUnresolvedDenies());
          rolePolicyLock.writeLock().lock();
          try {
            RoleLoadState state = states.get(id);
            CachedRolePolicies latest = state.policies;
            if (state.invalidatedAt > generation) {
              unreadable.add(id);
              continue;
            }
            if (latest != null
                && (latest.getUpdatedAt() > candidate.getUpdatedAt()
                    || (latest.getUpdatedAt() == candidate.getUpdatedAt()
                        && latest.isComplete()))) {
              policies.put(id, latest);
              continue;
            }
            state.policies = candidate;
            loadedRoles.put(id, candidate);
            if (candidate.isComplete()) {
              partialRoleLoadBackoff.invalidate(id);
            } else {
              partialRoleLoadBackoff.put(id, Boolean.TRUE);
            }
            // A newer snapshot must also refresh other in-flight requests. TTL eviction itself
            // never changes policy generations because requests pin immutable data.
            recordRoleClearGeneration(id);
            policies.put(id, candidate);
          } finally {
            rolePolicyLock.writeLock().unlock();
          }
        }
      }
      if (!unreadable.isEmpty()) {
        LOG.warn(
            "Cannot establish complete role reads for {}; new decisions fail closed", unreadable);
      }
      RequestRolePolicies view =
          new RequestRolePolicies(
              generation, policies, context.getActiveRoles(), versions, unreadable.isEmpty());
      rolePolicyLock.readLock().lock();
      try {
        // Pin the generation only after verifying the indexes selected throughout the load.
        // Publication can evict any cache entry, but active load states retain authoritative
        // values. A concurrent replacement/invalidation keeps the earlier generation, forcing
        // repair before this view can be evaluated. No metadata or database work runs here.
        boolean current =
            states.entrySet().stream()
                .allMatch(entry -> entry.getValue().policies == policies.get(entry.getKey()));
        if (current) {
          view.validatedAt(rolePolicyGenerationCounter.get());
        }
        context.setUnreadableRoleIds(unreadable);
        context.setRolePolicyView(view);
        context.setRolePolicyGeneration(view.generation());
      } finally {
        rolePolicyLock.readLock().unlock();
      }

    } finally {
      rolePolicyLock.writeLock().lock();
      try {
        states.forEach(
            (id, state) -> {
              if (--state.readers == 0) {
                loadingRoles.remove(id);
              }
            });
      } finally {
        rolePolicyLock.writeLock().unlock();
      }
    }
  }

  private void invalidateRolePolicies(long roleId) {
    invalidateRolePolicies(roleId, false);
  }

  private void invalidateRolePolicies(long roleId, boolean recordEmptyRoleChange) {
    rolePolicyLock.writeLock().lock();
    try {
      boolean present = loadedRoles.getIfPresent(roleId).isPresent();
      loadedRoles.invalidate(roleId);
      partialRoleLoadBackoff.invalidate(roleId);
      RoleLoadState loading = loadingRoles.get(roleId);
      if (present || recordEmptyRoleChange || (loading != null && loading.policies != null)) {
        recordRoleClearGeneration(roleId);
      }
      if (loading != null) {
        // Also invalidate loaders of an initially empty role. No stale resolution may publish
        // after explicit invalidation, even when neither cache nor request had seen policies.
        loading.policies = null;
        loading.invalidatedAt = rolePolicyGenerationCounter.incrementAndGet();
      }
    } finally {
      rolePolicyLock.writeLock().unlock();
    }
  }

  /**
   * Records a new clear generation for a role, pruning the older half of the generations when the
   * map outgrows {@link #maxRoleClearGenerations}. Must be called under the write lock of {@link
   * #rolePolicyLock}, so that readers see the pruned entries and the raised {@link
   * #prunedRoleClearGeneration} together.
   */
  private void recordRoleClearGeneration(long roleId) {
    roleClearGenerations.put(roleId, rolePolicyGenerationCounter.incrementAndGet());
    if (roleClearGenerations.size() <= maxRoleClearGenerations) {
      return;
    }
    long[] generations =
        roleClearGenerations.values().stream().mapToLong(Long::longValue).sorted().toArray();
    long cutoff = generations[generations.length / 2];
    roleClearGenerations.values().removeIf(generation -> generation <= cutoff);
    prunedRoleClearGeneration = cutoff;
  }

  /** Evaluates a pinned view, repairing explicit policy changes before any new decision. */
  private Optional<Boolean> evaluateWithLoadedRolePolicies(
      String metalake,
      AuthorizationRequestContext context,
      Function<RequestRolePolicies, Boolean> evaluation) {
    for (int reloads = 0; ; reloads++) {
      rolePolicyLock.readLock().lock();
      try {
        RequestRolePolicies view = (RequestRolePolicies) context.getRolePolicyView();
        if (view == null || !view.isReadable()) {
          return Optional.empty();
        }
        if (!hasClearedBoundRole(context, view.generation())) {
          long currentGeneration = rolePolicyGenerationCounter.get();
          if (view.generation() != currentGeneration) {
            view.validatedAt(currentGeneration);
            context.setRolePolicyGeneration(currentGeneration);
          }
          return Optional.of(evaluation.apply(view));
        }
      } finally {
        rolePolicyLock.readLock().unlock();
      }
      if (reloads == MAX_ROLE_POLICY_RELOADS) {
        LOG.warn(
            "Role policies changed during all {} reload attempts for metalake {}; failing closed. "
                + "Retry the request after role updates settle",
            MAX_ROLE_POLICY_RELOADS,
            metalake);
        return Optional.empty();
      }
      synchronized (context) {
        RequestRolePolicies view = (RequestRolePolicies) context.getRolePolicyView();
        rolePolicyLock.readLock().lock();
        boolean changed;
        try {
          changed = hasClearedBoundRole(context, view.generation());
        } finally {
          rolePolicyLock.readLock().unlock();
        }
        if (!changed) {
          continue;
        }
        long generation = rolePolicyGenerationCounter.get();
        try {
          loadRequestPolicies(metalake, context, generation);
        } catch (RuntimeException e) {
          LOG.warn("Failed to refresh role policies; failing closed", e);
          return Optional.empty();
        }
      }
    }
  }

  /** Must be called under the policy read lock. The usual unchanged-generation path is O(1). */
  private boolean hasClearedBoundRole(AuthorizationRequestContext context, long generation) {
    if (generation == rolePolicyGenerationCounter.get()) {
      return false;
    }
    if (!context.getBoundRoleIds().isEmpty() && generation < prunedRoleClearGeneration) {
      return true;
    }
    for (long id : context.getBoundRoleIds()) {
      Long changed = roleClearGenerations.get(id);
      if (changed != null && changed > generation) {
        return true;
      }
    }
    return false;
  }

  // ---------------------------------------------------------------------------
  //  Policy loading from role entity
  // ---------------------------------------------------------------------------

  /**
   * Resolves a role into an index outside the publication lock. Unresolved DENY privileges are
   * retained in a summary even when their exact object no longer resolves, so incomplete loads
   * cannot enable the list shortcut.
   *
   * <p>A securable object that cannot be resolved to a metadata id is reported in {@link
   * ResolvedRolePolicies#getUnresolvedObjects()} rather than silently dropped. That normally means
   * the object has been dropped while the role still references it. Transient normalization
   * failures also leave the role incomplete, but require a conservative catalog-scoped guard for
   * DENY privileges. If that guard cannot be installed, returns empty so the caller marks the role
   * unreadable without publishing any of its policies. Completeness belongs to the cached index,
   * independently of the retry throttle.
   */
  private Optional<ResolvedRolePolicies> resolveRolePolicies(
      RoleEntity roleEntity, AuthorizationRequestContext requestContext) {
    String metalake = NameIdentifierUtil.getMetalake(roleEntity.nameIdentifier());
    List<SecurableObject> securableObjects = roleEntity.securableObjects();

    Map<PolicyKey, Effect> index = new HashMap<>();
    Set<String> unresolvedDenies = new HashSet<>();
    List<String> unresolvedObjects = new ArrayList<>();

    for (SecurableObject securableObject : securableObjects) {
      JcasbinAuthorizationLookups.MetadataIdResolution resolution =
          lookups.resolveMetadataIdResult(securableObject, metalake, requestContext);
      Optional<Long> metadataId = resolution.metadataId();
      if (!metadataId.isPresent()) {
        unresolvedObjects.add(securableObject.type().name() + ":" + securableObject.fullName());
        for (Privilege privilege : securableObject.privileges()) {
          if (privilege.condition() == Privilege.Condition.DENY) {
            unresolvedDenies.add(
                AuthorizationUtils.replaceLegacyPrivilegeName(privilege.name()).name());
          }
        }
        if (resolution.normalizationFailed()
            && !addUnresolvedDenyPolicies(securableObject, metalake, requestContext, index)) {
          return Optional.empty();
        }
        continue;
      }
      addPolicies(securableObject.type(), metadataId.get(), securableObject.privileges(), index);
    }
    return Optional.of(new ResolvedRolePolicies(index, unresolvedObjects, unresolvedDenies));
  }

  private boolean addUnresolvedDenyPolicies(
      SecurableObject object,
      String metalake,
      AuthorizationRequestContext requestContext,
      Map<PolicyKey, Effect> index) {
    List<Privilege> denies =
        object.privileges().stream()
            .filter(privilege -> privilege.condition() == Privilege.Condition.DENY)
            .collect(Collectors.toList());
    if (denies.isEmpty()) {
      return true;
    }
    // An unresolved child DENY must not disappear while an ancestor or another role grants
    // ALLOW. Conservatively deny the same privileges throughout its catalog until the existing
    // partial-role retry resolves the precise object again. Catalog IDs need no capability lookup,
    // and other catalogs remain usable. A missing entity alone does not broaden existing policies.
    MetadataObject catalog =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.getCatalogIdentifier(
                MetadataObjectUtil.toEntityIdent(metalake, object)),
            Entity.EntityType.CATALOG);
    Optional<Long> catalogId = lookups.resolveMetadataId(catalog, metalake, requestContext);
    if (!catalogId.isPresent()) {
      // Without a catalog ID even a catalog-scoped guard cannot be installed safely. A missing
      // catalog makes normalization report a missing object instead, so this is only reachable
      // when the catalog is dropped between the two lookups. Skipping the DENY could drop it for
      // a catalog recreated under the same name. Mark the role unreadable and publish none of
      // its policies; the next request retries.
      LOG.warn("Cannot resolve catalog for unresolved deny policy on {}", object.fullName());
      return false;
    }
    addPolicies(MetadataObject.Type.CATALOG, catalogId.get(), denies, index);
    return true;
  }

  private static void addPolicies(
      MetadataObject.Type type,
      long metadataId,
      List<Privilege> privileges,
      Map<PolicyKey, Effect> index) {
    for (Privilege privilege : privileges) {
      String action = AuthorizationUtils.replaceLegacyPrivilegeName(privilege.name()).name();
      Effect effect =
          privilege.condition() == Privilege.Condition.DENY ? Effect.DENY : Effect.ALLOW;
      index.merge(
          new PolicyKey(type.name(), metadataId, action),
          effect,
          (old, next) -> old == Effect.DENY ? old : next);
    }
  }
}
