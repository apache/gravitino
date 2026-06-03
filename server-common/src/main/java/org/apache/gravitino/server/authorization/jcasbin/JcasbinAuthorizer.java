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
import java.util.LinkedHashSet;
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
 *       correctness — TTL eviction only bounds memory. User/group role snapshots use write-based
 *       TTLs through {@link CaffeineGravitinoCache}; loaded role policies use access-based TTLs
 *       through {@link JcasbinLoadedRolesCache}.
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
 * <p>JCasbin enforcer state ({@link #allowEnforcer}/{@link #denyEnforcer}) is kept in sync with
 * {@link #loadedRoles} via the removal listener inside {@link JcasbinLoadedRolesCache} — evicting a
 * role id also deletes that role's policies from both enforcers.
 */
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

  /**
   * loadedRoles: roleId -> updated_at. If the DB updated_at is newer, evict and reload policies.
   */
  private GravitinoCache<Long, Long> loadedRoles;

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
    changePoller = new JcasbinChangeListener(metadataIdCache, ownerRelCache, pollIntervalSecs);
    EntityStore entityStore = GravitinoEnv.getInstance().entityStore();
    if (entityStore instanceof SupportsEntityChangeLog) {
      ((SupportsEntityChangeLog) entityStore).registerEntityChangeLogListener(changePoller);
    }
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
  public boolean isSelf(
      Entity.EntityType type,
      NameIdentifier nameIdentifier,
      AuthorizationRequestContext requestContext) {
    String metalake = nameIdentifier.namespace().level(0);
    String currentUserName = PrincipalUtils.getCurrentUserName();
    if (Entity.EntityType.USER == type) {
      return Objects.equals(nameIdentifier.name(), currentUserName);
    } else if (Entity.EntityType.ROLE == type) {
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
          List<Long> groupRoleIds = loadGroupRoles(metalake, groupname, userId, requestContext);
          if (groupRoleIds.contains(resolvedRoleId)) {
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
      // OWNER does not consult JCasbin policies — it short-circuits to the owner cache in
      // authorizeByJcasbin. Skip the fat prefetch and role-binding work when no non-OWNER
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
        LOG.debug("Can not get entity id", e);
        return false;
      }

      if (!ownerOnly) {
        // Steps 1b→3: version-validated role loading (skipped for OWNER-only requests since
        // the enforcer is not consulted on the OWNER short-circuit).
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
     * {@link #authorizeByJcasbin}. A missing object is treated as "not authorized".
     */
    private boolean authorizeObject(
        long userId,
        String metalake,
        MetadataObject metadataObject,
        String privilege,
        AuthorizationRequestContext requestContext) {
      Optional<Long> metadataId =
          lookups.resolveMetadataId(metadataObject, metalake, requestContext);
      if (!metadataId.isPresent()) {
        return false;
      }
      return authorizeByJcasbin(
          userId, metalake, metadataObject, metadataId.get(), privilege, requestContext);
    }

    private boolean authorizeByJcasbin(
        long userId,
        String metalake,
        MetadataObject metadataObject,
        Long metadataId,
        String privilege,
        AuthorizationRequestContext requestContext) {
      // Step 4: JCasbin enforce (pure in-memory) — except OWNER, which is resolved via the
      // owner cache rather than g-rows.
      if (AuthConstants.OWNER.equals(privilege)) {
        // Cold-path: resolveOwnerId loads from DB when neither the per-request nor the shared
        // Caffeine cache has the entry, ensuring the first OWNER check doesn't spuriously deny.
        Optional<OwnerInfo> owner =
            lookups.resolveOwnerId(metadataId, metadataObject.type(), requestContext);
        return ownerMatchesUserOrGroups(
            owner, PrincipalUtils.getCurrentPrincipal(), metalake, requestContext);
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
   * AND the role-version probe inside {@link #versionCheckAndLoadRoles} into a single SQL round
   * trip. After this returns, the following caches are primed and the rest of the authorize hot
   * path needs zero DB round trips when the cached role policies are still current:
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
   *       consumed by {@link #versionCheckAndLoadRoles} to skip its dedicated probe.
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
    String userCacheKey = JcasbinAuthorizationCacheKeys.userRoleKey(metalake, username);
    Optional<CachedUserRoleRels> cachedOpt = userRoleCache.getIfPresent(userCacheKey);

    if (cachedOpt.isPresent()
        && cachedOpt.get().getUserId() == userId
        && cachedOpt.get().getUpdatedAt() >= userInfo.getUpdatedAt()) {
      // Cache is still valid. The user id check prevents reusing roles after deleting and
      // recreating the same username with a new entity id.
      CachedUserRoleRels cached = cachedOpt.get();
      bindUserRoles(userId, cached.getRoleIds());
      return cached.getRoleIds();
    }

    // Cache miss or stale — reload from DB
    List<RolePO> rolePOs =
        SessionUtils.getWithoutCommit(RoleMetaMapper.class, m -> m.listRolesByUserId(userId));
    List<Long> roleIds = rolePOs.stream().map(RolePO::getRoleId).collect(Collectors.toList());

    userRoleCache.put(
        userCacheKey, new CachedUserRoleRels(userId, userInfo.getUpdatedAt(), roleIds));
    bindUserRoles(userId, roleIds);
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
   * delete-and-create of the same group name. In both cases the resulting role IDs are bound to the
   * user's jcasbin g-rows so that the enforcer sees inherited privileges. Groups missing from the
   * DB return an empty list.
   */
  private List<Long> loadGroupRoles(
      String metalake, String groupname, long userId, AuthorizationRequestContext requestContext) {
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
        bindUserRoles(userId, cached.getRoleIds());
        return cached.getRoleIds();
      }
    }

    List<RolePO> rolePOs =
        SessionUtils.getWithoutCommit(RoleMetaMapper.class, m -> m.listRolesByGroupId(groupId));
    List<Long> roleIds = rolePOs.stream().map(RolePO::getRoleId).collect(Collectors.toList());

    groupRoleCache.put(
        groupCacheKey, new CachedGroupRoleRels(groupId, groupInfo.getUpdatedAt(), roleIds));
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
   * not found in the store. Used by owner checks that need full group entities instead of only
   * group names.
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
    List<Long> uniqueRoleIds = roleIds.stream().distinct().collect(Collectors.toList());

    Map<Long, RoleUpdatedAt> prefetched = requestContext.getPrefetchedRoleVersions();
    List<RoleUpdatedAt> roleVersions = new ArrayList<>(uniqueRoleIds.size());
    List<Long> missingRoleIds = new ArrayList<>();
    for (Long rid : uniqueRoleIds) {
      RoleUpdatedAt rv = prefetched == null ? null : prefetched.get(rid);
      if (rv != null) {
        roleVersions.add(rv);
      } else {
        missingRoleIds.add(rid);
      }
    }
    if (!missingRoleIds.isEmpty()) {
      roleVersions.addAll(
          SessionUtils.getWithoutCommit(
              RoleMetaMapper.class, m -> m.batchGetRoleUpdatedAt(missingRoleIds)));
    }

    // Any roleId asked about but not returned has been deleted in the DB; clear its policies so
    // a stale grouping row in the enforcer can't keep granting privileges before the next
    // userRoleCache reload prunes the g-row itself.
    Set<Long> existingRoleIds = new HashSet<>(roleVersions.size());
    for (RoleUpdatedAt rv : roleVersions) {
      existingRoleIds.add(rv.getRoleId());
    }
    for (Long roleId : uniqueRoleIds) {
      if (!existingRoleIds.contains(roleId)) {
        clearRolePolicies(roleId);
        loadedRoles.invalidate(roleId);
      }
    }

    List<RoleUpdatedAt> staleRoleVersions = new ArrayList<>();
    for (RoleUpdatedAt rv : roleVersions) {
      Optional<Long> cachedUpdatedAt = loadedRoles.getIfPresent(rv.getRoleId());
      if (cachedUpdatedAt.isPresent() && cachedUpdatedAt.get() >= rv.getUpdatedAt()) {
        continue;
      }
      staleRoleVersions.add(rv);
    }

    if (staleRoleVersions.isEmpty()) {
      return;
    }

    EntityStore entityStore = GravitinoEnv.getInstance().entityStore();
    List<NameIdentifier> roleIdents =
        staleRoleVersions.stream()
            .map(rv -> NameIdentifierUtil.ofRole(metalake, rv.getRoleName()))
            .collect(Collectors.toList());
    List<RoleEntity> roleEntities;
    try {
      roleEntities = entityStore.batchGet(roleIdents, Entity.EntityType.ROLE, RoleEntity.class);
    } catch (Exception e) {
      LOG.warn("Failed to batch load stale role policies for roleIds {}", staleRoleVersions, e);
      roleEntities = new ArrayList<>();
    }
    if (roleEntities == null) {
      roleEntities = new ArrayList<>();
    }
    // Some EntityStore implementations don't support batchGet for ROLE and return empty;
    // fall back to per-role get so policies still load.
    if (roleEntities.isEmpty()) {
      roleEntities = new ArrayList<>(staleRoleVersions.size());
      for (RoleUpdatedAt rv : staleRoleVersions) {
        try {
          roleEntities.add(
              entityStore.get(
                  NameIdentifierUtil.ofRole(metalake, rv.getRoleName()),
                  Entity.EntityType.ROLE,
                  RoleEntity.class));
        } catch (Exception e) {
          LOG.warn("Failed to load role policies for roleId {}", rv.getRoleId(), e);
        }
      }
    }

    Map<Long, RoleUpdatedAt> staleRoleVersionById =
        staleRoleVersions.stream().collect(Collectors.toMap(RoleUpdatedAt::getRoleId, rv -> rv));
    for (RoleEntity roleEntity : roleEntities) {
      if (roleEntity == null) {
        continue;
      }
      RoleUpdatedAt rv = staleRoleVersionById.get(roleEntity.id());
      if (rv == null) {
        continue;
      }
      long roleId = rv.getRoleId();
      long dbUpdatedAt = rv.getUpdatedAt();
      Optional<Long> cachedUpdatedAt = loadedRoles.getIfPresent(roleId);

      // Refresh only permission policies. deleteRole would also remove the current user's freshly
      // bound grouping links.
      if (cachedUpdatedAt.isPresent()) {
        clearRolePolicies(roleId);
      }
      loadPolicyByRoleEntity(roleEntity, requestContext);
      loadedRoles.put(roleId, dbUpdatedAt);
    }
  }

  private void clearRolePolicies(long roleId) {
    String roleIdStr = String.valueOf(roleId);
    allowEnforcer.removeFilteredPolicy(0, roleIdStr);
    denyEnforcer.removeFilteredPolicy(0, roleIdStr);
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
      Optional<Long> metadataId =
          lookups.resolveMetadataId(securableObject, metalake, requestContext);
      // A role may still reference a metadata object that has since been dropped; skip it.
      if (!metadataId.isPresent()) {
        continue;
      }
      for (Privilege privilege : securableObject.privileges()) {
        Privilege.Condition condition = privilege.condition();
        if (AuthConstants.DENY.equalsIgnoreCase(condition.name())) {
          denyEnforcer.addPolicy(
              String.valueOf(roleEntity.id()),
              securableObject.type().name(),
              String.valueOf(metadataId.get()),
              AuthorizationUtils.replaceLegacyPrivilegeName(privilege.name())
                  .name()
                  .toUpperCase(Locale.ROOT),
              AuthConstants.ALLOW);
        }

        allowEnforcer.addPolicy(
            String.valueOf(roleEntity.id()),
            securableObject.type().name(),
            String.valueOf(metadataId.get()),
            AuthorizationUtils.replaceLegacyPrivilegeName(privilege.name())
                .name()
                .toUpperCase(Locale.ROOT),
            condition.name().toLowerCase(Locale.ROOT));
      }
    }
  }
}
