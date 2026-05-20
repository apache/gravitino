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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.storage.relational.po.auth.GroupUpdatedAt;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.po.auth.UserUpdatedAt;

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
 * <p>Instances are not intended to outlive a request and are not reusable across threads beyond the
 * request handling thread; the internal maps are {@link ConcurrentHashMap} purely to tolerate any
 * incidental fan-out (e.g. async listeners) within the same request scope.
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

  private volatile String originalAuthorizationExpression;

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
