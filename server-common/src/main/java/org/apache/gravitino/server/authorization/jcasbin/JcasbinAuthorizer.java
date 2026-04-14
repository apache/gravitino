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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
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
import org.apache.gravitino.cache.CaffeineGravitinoCache;
import org.apache.gravitino.cache.GravitinoCache;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.RoleMetaMapper;
import org.apache.gravitino.storage.relational.mapper.UserMetaMapper;
import org.apache.gravitino.storage.relational.po.OwnerRelInfoPO;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.gravitino.storage.relational.po.RoleVersionInfoPO;
import org.apache.gravitino.storage.relational.po.SecurableObjectPO;
import org.apache.gravitino.storage.relational.po.UserVersionInfoPO;
import org.apache.gravitino.storage.relational.service.RoleMetaService;
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

  /** Jcasbin enforcer is used for metadata authorization. */
  private Enforcer allowEnforcer;

  /** Jcasbin deny enforcer is used for metadata authorization. */
  private Enforcer denyEnforcer;

  /** allow internal authorizer */
  private InternalAuthorizer allowInternalAuthorizer;

  /** deny internal authorizer */
  private InternalAuthorizer denyInternalAuthorizer;

  /**
   * Per-role policy version cache. Maps roleId → securableObjectsVersion. When the version in the
   * DB differs from the cached value, the role's policies are reloaded. The removal listener clears
   * stale policies from both enforcers on eviction.
   */
  private GravitinoCache<Long, Integer> loadedRoles;

  /**
   * Per-user role cache. Maps "metalake:username" → CachedUserRoles (userId, roleGrantsVersion,
   * roleIds). Validated against the DB version on every request before use.
   */
  private GravitinoCache<String, CachedUserRoles> userRoleCache;

  /** Holds a snapshot of user role assignments for a single request lifetime. */
  private static final class CachedUserRoles {
    final long userId;
    final int roleGrantsVersion;
    final List<Long> roleIds;

    CachedUserRoles(long userId, int roleGrantsVersion, List<Long> roleIds) {
      this.userId = userId;
      this.roleGrantsVersion = roleGrantsVersion;
      this.roleIds = roleIds;
    }
  }

  @Override
  public void initialize() {
    long cacheExpirationSecs =
        GravitinoEnv.getInstance()
            .config()
            .get(Configs.GRAVITINO_AUTHORIZATION_CACHE_EXPIRATION_SECS);
    long roleCacheSize =
        GravitinoEnv.getInstance().config().get(Configs.GRAVITINO_AUTHORIZATION_ROLE_CACHE_SIZE);
    long userRoleCacheSize =
        GravitinoEnv.getInstance()
            .config()
            .get(Configs.GRAVITINO_AUTHORIZATION_USER_ROLE_CACHE_SIZE);

    // Initialize enforcers before the caches that reference them in removal listeners
    allowEnforcer = new SyncedEnforcer(getModel("/jcasbin_model.conf"), new GravitinoAdapter());
    allowInternalAuthorizer = new InternalAuthorizer(allowEnforcer);
    denyEnforcer = new SyncedEnforcer(getModel("/jcasbin_model.conf"), new GravitinoAdapter());
    denyInternalAuthorizer = new InternalAuthorizer(denyEnforcer);

    loadedRoles =
        new CaffeineGravitinoCache<>(
            roleCacheSize,
            cacheExpirationSecs,
            (roleId, version, cause) -> {
              if (roleId != null) {
                allowEnforcer.deleteRole(String.valueOf(roleId));
                denyEnforcer.deleteRole(String.valueOf(roleId));
              }
            });

    userRoleCache = new CaffeineGravitinoCache<>(userRoleCacheSize, cacheExpirationSecs);
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
    boolean result;
    try {
      Long metadataId = MetadataIdConverter.getID(metadataObject, metalake);

      // Resolve userId — use cached value if this request already resolved it
      Long userId = requestContext.getResolvedUserId();
      if (userId == null) {
        UserVersionInfoPO versionInfo =
            SessionUtils.getWithoutCommit(
                UserMetaMapper.class,
                mapper -> mapper.getUserVersionInfo(metalake, principal.getName()));
        if (versionInfo == null) {
          return false;
        }
        userId = versionInfo.getUserId();
        requestContext.setResolvedUserId(userId);
      }

      // Resolve owner — use per-request cache to deduplicate DB lookups across ancestor chain
      Optional<Long> ownerOpt;
      if (requestContext.isOwnerCached(metadataId)) {
        ownerOpt = requestContext.getCachedOwner(metadataId);
      } else {
        OwnerRelInfoPO ownerRow =
            SessionUtils.getWithoutCommit(
                OwnerMetaMapper.class, mapper -> mapper.selectOwnerByMetadataObjectId(metadataId));
        if (ownerRow == null || !"USER".equals(ownerRow.getOwnerType())) {
          ownerOpt = Optional.empty();
        } else {
          ownerOpt = Optional.of(ownerRow.getOwnerId());
        }
        requestContext.cacheOwner(metadataId, ownerOpt);
      }

      result = ownerOpt.isPresent() && Objects.equals(userId, ownerOpt.get());
    } catch (Exception e) {
      LOG.debug("Can not get entity id for owner check", e);
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
        List<RoleEntity> entities =
            GravitinoEnv.getInstance()
                .entityStore()
                .relationOperations()
                .listEntitiesByRelation(
                    SupportsRelationOperations.Type.ROLE_USER_REL,
                    NameIdentifierUtil.ofUser(metalake, PrincipalUtils.getCurrentUserName()),
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
    MetadataObject.Type metadataType;
    try {
      metadataType = MetadataObject.Type.valueOf(type.toUpperCase());
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

  @Override
  public void handleRolePrivilegeChange(Long roleId) {
    loadedRoles.invalidate(roleId);
  }

  /**
   * No-op: owner lookups are now done per-request via the DB, without a long-lived owner cache.
   * This hook is kept for interface compatibility.
   */
  @Override
  public void handleMetadataOwnerChange(
      String metalake, Long oldOwnerId, NameIdentifier nameIdentifier, Entity.EntityType type) {
    // No persistent owner cache to invalidate — owner is resolved fresh per-request.
  }

  @Override
  public void close() throws IOException {
    if (loadedRoles != null) {
      loadedRoles.close();
    }
    if (userRoleCache != null) {
      userRoleCache.close();
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
      try {
        metadataId = MetadataIdConverter.getID(metadataObject, metalake);
      } catch (Exception e) {
        LOG.debug("Can not get metadata id", e);
        return false;
      }
      loadRolePrivilege(metalake, username, requestContext);
      Long userId = requestContext.getResolvedUserId();
      if (userId == null) {
        return false;
      }
      return authorizeByJcasbin(userId, metadataObject, metadataId, privilege, requestContext);
    }

    private boolean authorizeByJcasbin(
        Long userId,
        MetadataObject metadataObject,
        Long metadataId,
        String privilege,
        AuthorizationRequestContext requestContext) {
      if (AuthConstants.OWNER.equals(privilege)) {
        Optional<Long> ownerOpt = requestContext.getCachedOwner(metadataId);
        return ownerOpt != null && ownerOpt.isPresent() && Objects.equals(userId, ownerOpt.get());
      }
      return enforcer.enforce(
          String.valueOf(userId),
          String.valueOf(metadataObject.type()),
          String.valueOf(metadataId),
          privilege);
    }
  }

  /**
   * Version-validated role loading. Runs at most once per request (guarded by
   * requestContext.loadRole). On each execution:
   *
   * <ol>
   *   <li>Fetches userId and roleGrantsVersion from DB.
   *   <li>Compares DB version with userRoleCache; reloads role IDs if stale.
   *   <li>For each role, compares DB securableObjectsVersion with loadedRoles; reloads policies if
   *       stale.
   *   <li>Wires user→role relationships in both JCasbin enforcers.
   * </ol>
   */
  private void loadRolePrivilege(
      String metalake, String username, AuthorizationRequestContext requestContext) {
    requestContext.loadRole(
        () -> {
          // Step 1: Fetch userId + roleGrantsVersion from DB
          UserVersionInfoPO versionInfo =
              SessionUtils.getWithoutCommit(
                  UserMetaMapper.class, mapper -> mapper.getUserVersionInfo(metalake, username));
          if (versionInfo == null) {
            LOG.debug("User {} not found in metalake {}", username, metalake);
            return;
          }
          long userId = versionInfo.getUserId();
          int dbRoleGrantsVersion = versionInfo.getRoleGrantsVersion();
          requestContext.setResolvedUserId(userId);

          // Step 2: Check/update userRoleCache
          String cacheKey = metalake + ":" + username;
          CachedUserRoles cached = userRoleCache.getIfPresent(cacheKey).orElse(null);

          List<Long> roleIds;
          boolean roleListStale = cached == null || cached.roleGrantsVersion != dbRoleGrantsVersion;
          if (!roleListStale) {
            roleIds = cached.roleIds;
          } else {
            List<RolePO> rolePOs =
                SessionUtils.getWithoutCommit(
                    RoleMetaMapper.class, mapper -> mapper.listRolesByUserId(userId));
            roleIds = rolePOs.stream().map(RolePO::getRoleId).collect(Collectors.toList());
            userRoleCache.put(cacheKey, new CachedUserRoles(userId, dbRoleGrantsVersion, roleIds));
            // Clear stale user→role links in JCasbin before re-wiring
            allowEnforcer.deleteRolesForUser(String.valueOf(userId));
            denyEnforcer.deleteRolesForUser(String.valueOf(userId));
          }

          if (roleIds.isEmpty()) {
            return;
          }

          // Step 3: Check per-role policy versions, reload stale roles
          Map<Long, Integer> dbRoleVersions = fetchRoleVersionsFromDB(roleIds);
          for (Long roleId : roleIds) {
            Integer dbVersion = dbRoleVersions.get(roleId);
            if (dbVersion == null) {
              // Role may have been deleted; skip
              continue;
            }
            Optional<Integer> cachedVersion = loadedRoles.getIfPresent(roleId);
            if (cachedVersion.isPresent() && cachedVersion.get().intValue() == dbVersion) {
              // Policies for this role are up-to-date
              continue;
            }
            // Invalidate first: triggers the removal listener which deletes stale policies from
            // JCasbin. Then re-add fresh policies. Calling put() directly would also trigger the
            // listener on REPLACED, wiping the newly loaded policies.
            loadedRoles.invalidate(roleId);
            List<SecurableObjectPO> pos = RoleMetaService.listSecurableObjectsByRoleId(roleId);
            loadPoliciesFromPOs(roleId, pos);
            loadedRoles.put(roleId, dbVersion);
          }

          // Step 4: Wire user → roles in JCasbin
          for (Long roleId : roleIds) {
            allowEnforcer.addRoleForUser(String.valueOf(userId), String.valueOf(roleId));
            denyEnforcer.addRoleForUser(String.valueOf(userId), String.valueOf(roleId));
          }
        });
  }

  private Map<Long, Integer> fetchRoleVersionsFromDB(List<Long> roleIds) {
    List<RoleVersionInfoPO> rows =
        SessionUtils.getWithoutCommit(
            RoleMetaMapper.class, mapper -> mapper.batchGetSecurableObjectsVersions(roleIds));
    Map<Long, Integer> result = new HashMap<>();
    for (RoleVersionInfoPO row : rows) {
      result.put(row.getRoleId(), row.getSecurableObjectsVersion());
    }
    return result;
  }

  /**
   * Loads privilege policies for a role directly from {@link SecurableObjectPO} records, avoiding
   * the need to resolve integer metadata IDs through {@link MetadataIdConverter}.
   *
   * <p>DENY-condition privileges are added to the denyEnforcer with "allow" effect, and also to
   * allowEnforcer with "deny" effect so that concurrent allow+deny grants on the same object return
   * false.
   */
  @SuppressWarnings("unchecked")
  private void loadPoliciesFromPOs(long roleId, List<SecurableObjectPO> pos) {
    String roleIdStr = String.valueOf(roleId);
    for (SecurableObjectPO po : pos) {
      List<String> privilegeNames;
      List<String> privilegeConditions;
      try {
        privilegeNames = JsonUtils.anyFieldMapper().readValue(po.getPrivilegeNames(), List.class);
        privilegeConditions =
            JsonUtils.anyFieldMapper().readValue(po.getPrivilegeConditions(), List.class);
      } catch (JsonProcessingException e) {
        LOG.warn(
            "Failed to parse privilege JSON for role {} securable object {}: {}",
            roleId,
            po.getMetadataObjectId(),
            e.getMessage());
        continue;
      }

      String objectType = po.getType();
      String objectIdStr = String.valueOf(po.getMetadataObjectId());

      for (int i = 0; i < privilegeNames.size(); i++) {
        String rawPrivName = privilegeNames.get(i);
        String condition = privilegeConditions.get(i);

        String normalizedPrivName =
            AuthorizationUtils.replaceLegacyPrivilegeName(
                    Privilege.Name.valueOf(rawPrivName.toUpperCase(Locale.ROOT)))
                .name()
                .toUpperCase(Locale.ROOT);

        if (AuthConstants.DENY.equalsIgnoreCase(condition)) {
          denyEnforcer.addPolicy(
              roleIdStr, objectType, objectIdStr, normalizedPrivName, AuthConstants.ALLOW);
        }
        // allowEnforcer carries both ALLOW and DENY effects so that deny overrides allow
        allowEnforcer.addPolicy(
            roleIdStr,
            objectType,
            objectIdStr,
            normalizedPrivName,
            condition.toLowerCase(Locale.ROOT));
      }
    }
  }
}
