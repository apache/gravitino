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

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.ActiveRoles;
import org.apache.gravitino.storage.relational.po.auth.GroupUpdatedAt;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.po.auth.RoleUpdatedAt;
import org.apache.gravitino.storage.relational.po.auth.UserUpdatedAt;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * Per-HTTP-request scratchpad shared by {@link GravitinoAuthorizer} calls. A fresh instance is
 * created for each request by the authorization filter and threaded through {@code authorize},
 * {@code isOwner}, {@code isMetalakeUser} etc., so that:
 *
 * <ul>
 *   <li>repeated authorization decisions for the same {@code (principal, metalake, object,
 *       privilege)} short-circuit via {@link #allowAuthorizerCache} / {@link #denyAuthorizerCache};
 *   <li>user identity, name→id and metadataId→owner lookups are de-duplicated within the request
 *       (see the {@code computeXxxIfAbsent} helpers) so each underlying DB query runs at most once;
 *   <li>per-request role loading happens at most once via {@link #loadRole(Runnable)}.
 * </ul>
 *
 * <p>Instances must not outlive a request or be reused across principals, active-role selections or
 * metalakes. Entry authorization and list filtering of one read-only request may share an instance:
 * list workers receive it explicitly, which is why the internal maps are {@link ConcurrentHashMap}.
 * Role selection must be fixed before workers start, and a mutation must not reuse decisions made
 * before it.
 */
public class AuthorizationRequestContext {

  /** Used to cache the results of metadata authorization. */
  private final Map<AuthorizationKey, Boolean> allowAuthorizerCache = new ConcurrentHashMap<>();

  /** Used to cache the results of metadata authorization. */
  private final Map<AuthorizationKey, Boolean> denyAuthorizerCache = new ConcurrentHashMap<>();

  /** Used to determine whether the role has already been loaded. */
  private final AtomicBoolean hasLoadRole = new AtomicBoolean();

  /** Per-request user identity cache. Key: {@code metalake::userName}. */
  private final Map<String, Optional<UserUpdatedAt>> userInfoCache = new ConcurrentHashMap<>();

  /** Per-request group identity cache. Key: {@code metalake::groupName}. */
  private final Map<String, Optional<GroupUpdatedAt>> groupInfoCache = new ConcurrentHashMap<>();

  /** Per-request name→id cache. Deduplicates resolveMetadataId within a single request. */
  private final Map<String, Long> metadataIdCache = new ConcurrentHashMap<>();

  /** Per-request metadataId→owner cache. Deduplicates isOwner within a single request. */
  private final Map<Long, Optional<OwnerInfo>> ownerCache = new ConcurrentHashMap<>();

  /**
   * Per-request roleId → {@link RoleUpdatedAt} map populated by the fat-JOIN prefetch on the
   * authorize hot path. When present, {@code versionCheckAndLoadRoles} can skip its dedicated
   * {@code batchGetRoleUpdatedAt} round trip.
   */
  private volatile Map<Long, RoleUpdatedAt> prefetchedRoleVersions;

  private volatile String originalAuthorizationExpression;

  /**
   * Ids of the roles bound to the caller when this request loaded role policies. The authorizer
   * uses them to detect whether one of these roles lost its policies after the load.
   */
  private volatile List<Long> boundRoleIds = Collections.emptyList();

  /**
   * Authorizer-defined generation of the in-memory role policies this request last validated its
   * bound roles against. A role cleared after this generation must be reloaded before the request
   * evaluates it again.
   */
  private final AtomicLong rolePolicyGeneration = new AtomicLong();

  /** Roles whose entities could not be read during this request's initial role load. */
  private volatile Set<Long> unreadableRoleIds = Collections.emptySet();

  /**
   * The roles the caller has declared active for this request (role assumption). Read from the
   * current {@link UserPrincipal}; defaults to {@link ActiveRoles#all()} (no narrowing) when the
   * caller declared none.
   */
  private volatile ActiveRoles activeRoles = currentPrincipalActiveRoles();

  private static ActiveRoles currentPrincipalActiveRoles() {
    Principal principal = PrincipalUtils.getCurrentPrincipal();
    return principal instanceof UserPrincipal
        ? ((UserPrincipal) principal).getActiveRoles()
        : ActiveRoles.all();
  }

  /**
   * check allow
   *
   * @param principal principal
   * @param metalake metalake
   * @param metadataObject metadata object
   * @param privilege privilege
   * @param authorizer authorizer
   * @return authorization result
   */
  public boolean authorizeAllow(
      Principal principal,
      String metalake,
      MetadataObject metadataObject,
      Privilege.Name privilege,
      Function<AuthorizationKey, Boolean> authorizer) {
    AuthorizationKey context = new AuthorizationKey(principal, metalake, metadataObject, privilege);
    return allowAuthorizerCache.computeIfAbsent(context, authorizer);
  }

  /**
   * check deny
   *
   * @param principal principal
   * @param metalake metalake
   * @param metadataObject metadata object
   * @param privilege privilege
   * @param authorizer authorizer
   * @return authorization result
   */
  public boolean authorizeDeny(
      Principal principal,
      String metalake,
      MetadataObject metadataObject,
      Privilege.Name privilege,
      Function<AuthorizationKey, Boolean> authorizer) {
    AuthorizationKey context = new AuthorizationKey(principal, metalake, metadataObject, privilege);
    return denyAuthorizerCache.computeIfAbsent(context, authorizer);
  }

  /**
   * Runs {@code runnable} at most once per request. The double-checked guard plus {@code
   * synchronized(this)} prevents two concurrent authorize calls in the same request from both
   * triggering the (potentially expensive) role load.
   */
  public void loadRole(Runnable runnable) {
    if (hasLoadRole.get()) {
      return;
    }
    synchronized (this) {
      if (hasLoadRole.get()) {
        return;
      }
      try {
        runnable.run();
        hasLoadRole.set(true);
      } catch (Exception e) {
        throw new RuntimeException("Failed to load role: ", e);
      }
    }
  }

  /**
   * Per-request {@link UserUpdatedAt} dedup. Loader may return {@link Optional#empty()} to cache
   * the "user not found" outcome and avoid repeated DB lookups within a single request.
   */
  public Optional<UserUpdatedAt> computeUserInfoIfAbsent(
      String key, Function<String, Optional<UserUpdatedAt>> loader) {
    return userInfoCache.computeIfAbsent(
        key, k -> Objects.requireNonNull(loader.apply(k), "User info loader must not return null"));
  }

  /**
   * Per-request {@link GroupUpdatedAt} dedup. Loader may return {@link Optional#empty()} to cache
   * the "group not found" outcome and avoid repeated DB lookups within a single request.
   */
  public Optional<GroupUpdatedAt> computeGroupInfoIfAbsent(
      String key, Function<String, Optional<GroupUpdatedAt>> loader) {
    return groupInfoCache.computeIfAbsent(
        key,
        k -> Objects.requireNonNull(loader.apply(k), "Group info loader must not return null"));
  }

  /** Per-request name→id dedup. Loader must return a non-null id or throw. */
  public Long computeMetadataIdIfAbsent(String key, Function<String, Long> loader) {
    return metadataIdCache.computeIfAbsent(
        key,
        k -> Objects.requireNonNull(loader.apply(k), "Metadata id loader must not return null"));
  }

  /**
   * Per-request metadataId→owner dedup. Loader returns {@link Optional#empty()} when the object has
   * no owner; the absent result is cached as well.
   */
  public Optional<OwnerInfo> computeOwnerIfAbsent(
      Long metadataId, Function<Long, Optional<OwnerInfo>> loader) {
    return ownerCache.computeIfAbsent(
        metadataId,
        id -> Objects.requireNonNull(loader.apply(id), "Owner loader must not return null"));
  }

  public String getOriginalAuthorizationExpression() {
    return originalAuthorizationExpression;
  }

  public void setOriginalAuthorizationExpression(String originalAuthorizationExpression) {
    this.originalAuthorizationExpression = originalAuthorizationExpression;
  }

  /**
   * Returns the prefetched roleId → {@link RoleUpdatedAt} map, or {@code null} when the fat-JOIN
   * prefetch has not run for this request.
   *
   * @return the prefetched role-versions map or {@code null}
   */
  public Map<Long, RoleUpdatedAt> getPrefetchedRoleVersions() {
    return prefetchedRoleVersions;
  }

  /**
   * Sets the prefetched roleId → {@link RoleUpdatedAt} map; called once per request by the
   * authorize hot path after the fat-JOIN prefetch.
   *
   * @param prefetchedRoleVersions roleId → {@link RoleUpdatedAt} map
   */
  public void setPrefetchedRoleVersions(Map<Long, RoleUpdatedAt> prefetchedRoleVersions) {
    this.prefetchedRoleVersions = prefetchedRoleVersions;
  }

  /**
   * Returns the ids of the roles bound to the caller when this request loaded role policies.
   *
   * @return the bound role ids; empty when no role has been loaded yet
   */
  public List<Long> getBoundRoleIds() {
    return boundRoleIds;
  }

  /**
   * Records the ids of the roles bound to the caller by this request's role load.
   *
   * @param boundRoleIds the bound role ids; must not be {@code null}
   */
  public void setBoundRoleIds(List<Long> boundRoleIds) {
    this.boundRoleIds =
        Collections.unmodifiableList(
            new ArrayList<>(Objects.requireNonNull(boundRoleIds, "boundRoleIds must not be null")));
  }

  /**
   * Returns the role policy generation this request last validated its bound roles against.
   *
   * @return the role policy generation
   */
  public long getRolePolicyGeneration() {
    return rolePolicyGeneration.get();
  }

  /**
   * Advances the role policy generation this request validated its bound roles against. Concurrent
   * workers cannot move the recorded generation backwards.
   *
   * @param rolePolicyGeneration the role policy generation
   */
  public void setRolePolicyGeneration(long rolePolicyGeneration) {
    this.rolePolicyGeneration.accumulateAndGet(rolePolicyGeneration, Math::max);
  }

  /**
   * Returns the roles whose entities could not be read during the initial role load.
   *
   * @return the unreadable role ids; empty when all role entities were readable
   */
  public Set<Long> getUnreadableRoleIds() {
    return unreadableRoleIds;
  }

  /**
   * Records unreadable roles during the initial load so every check of this request fails closed.
   *
   * @param unreadableRoleIds the unreadable role ids; must not be {@code null}
   */
  public void setUnreadableRoleIds(Set<Long> unreadableRoleIds) {
    this.unreadableRoleIds =
        Collections.unmodifiableSet(
            new HashSet<>(
                Objects.requireNonNull(unreadableRoleIds, "unreadableRoleIds must not be null")));
  }

  /**
   * Returns the roles declared active for this request. Never {@code null}; defaults to {@link
   * ActiveRoles#all()}.
   *
   * @return the active-role declaration
   */
  public ActiveRoles getActiveRoles() {
    return activeRoles;
  }

  /**
   * Sets the roles declared active for this request. Narrowing is subtractive: only roles the
   * caller actually holds are ever consulted, regardless of what is declared here.
   *
   * @param activeRoles the active-role declaration; must not be {@code null}
   */
  public void setActiveRoles(ActiveRoles activeRoles) {
    this.activeRoles = Objects.requireNonNull(activeRoles, "activeRoles must not be null");
  }

  /**
   * Composite key for {@link #allowAuthorizerCache} / {@link #denyAuthorizerCache}. Immutable —
   * mutating any field after construction would silently corrupt the {@link
   * java.util.Objects#hashCode} used by the backing {@link ConcurrentHashMap}.
   */
  @Getter
  @AllArgsConstructor
  @EqualsAndHashCode
  public static class AuthorizationKey {
    private final Principal principal;
    private final String metalake;
    private final MetadataObject metadataObject;
    private final Privilege.Name privilege;
  }
}
