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
package org.apache.gravitino.server.authorization.jcasbin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.gravitino.auth.ActiveRoles;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.storage.relational.po.auth.RoleUpdatedAt;

/** Immutable role indexes pinned for one request, including inactive-role denies. */
final class RequestRolePolicies implements AuthorizationRequestContext.RolePolicyView {
  private final AtomicLong generation;
  private final List<CachedRolePolicies> allowRoles;
  private final List<CachedRolePolicies> denyRoles;
  private final Set<String> deniedPrivileges;
  private final boolean readable;
  private final AtomicLong remainingProbes;
  @Nullable private volatile Map<PolicyKey, Effect> mergedIndex;

  RequestRolePolicies(
      long generation,
      Map<Long, CachedRolePolicies> roles,
      ActiveRoles active,
      Map<Long, RoleUpdatedAt> versions) {
    this(generation, roles, active, versions, true);
  }

  RequestRolePolicies(
      long generation,
      Map<Long, CachedRolePolicies> roles,
      ActiveRoles active,
      Map<Long, RoleUpdatedAt> versions,
      boolean readable) {
    this.readable = readable;
    this.generation = new AtomicLong(generation);
    List<CachedRolePolicies> allow = new ArrayList<>();
    List<CachedRolePolicies> deny = new ArrayList<>();
    Set<String> denied = new HashSet<>();
    long policyCount = roles.values().stream().mapToLong(CachedRolePolicies::policyCount).sum();
    remainingProbes = new AtomicLong(policyCount);
    roles.forEach(
        (id, policies) -> {
          RoleUpdatedAt info = versions == null ? null : versions.get(id);
          // Named activation requires names and policies from the same role version. A racing
          // newer loader may supply the index before this request's older version probe returns.
          boolean enabled =
              active.isAll()
                  || (!active.isNone()
                      && info != null
                      && info.getUpdatedAt() == policies.getUpdatedAt()
                      && active.roleNames().contains(info.getRoleName()));
          // Pin immutable role indexes instead of copying every policy for a small request.
          // DENY remains global even when the role's ALLOW privileges are inactive.
          if (enabled) {
            allow.add(policies);
          }
          if (!policies.getDeniedPrivileges().isEmpty()) {
            deny.add(policies);
          }
          denied.addAll(policies.getDeniedPrivileges());
        });
    allowRoles = Collections.unmodifiableList(allow);
    denyRoles = Collections.unmodifiableList(deny);
    deniedPrivileges = Collections.unmodifiableSet(denied);
  }

  /** {@inheritDoc} */
  @Override
  public long generation() {
    return generation.get();
  }

  /** Advances only the validation watermark; policy indexes remain immutable. */
  void validatedAt(long value) {
    generation.accumulateAndGet(value, Math::max);
  }

  boolean isReadable() {
    return readable;
  }

  boolean evaluate(PolicyKey key, boolean allow) {
    if (!readable) {
      return !allow;
    }
    Map<PolicyKey, Effect> merged = mergedIndex;
    if (merged != null) {
      return merged.get(key) == (allow ? Effect.ALLOW : Effect.DENY);
    }
    long probes = 0L;
    try {
      for (CachedRolePolicies role : denyRoles) {
        probes++;
        if (role.getIndex().get(key) == Effect.DENY) {
          return !allow;
        }
      }
      if (allow) {
        for (CachedRolePolicies role : allowRoles) {
          probes++;
          if (role.getIndex().get(key) == Effect.ALLOW) {
            return true;
          }
        }
      }
      return false;
    } finally {
      // Large roles stay cheap for small requests. A long list switches to a merged index only
      // after its actual role probes have paid for scanning the pinned policies.
      if (probes > 0 && remainingProbes.getAndAdd(-probes) <= probes) {
        mergePolicies();
      }
    }
  }

  private synchronized void mergePolicies() {
    if (mergedIndex != null) {
      return;
    }
    Map<PolicyKey, Effect> merged = new HashMap<>();
    for (CachedRolePolicies role : allowRoles) {
      role.getIndex()
          .forEach(
              (key, effect) -> {
                if (effect == Effect.ALLOW) {
                  merged.put(key, Effect.ALLOW);
                }
              });
    }
    // All held DENYs overwrite active ALLOWs, regardless of role activation.
    for (CachedRolePolicies role : denyRoles) {
      role.getIndex()
          .forEach(
              (key, effect) -> {
                if (effect == Effect.DENY) {
                  merged.put(key, Effect.DENY);
                }
              });
    }
    mergedIndex = Collections.unmodifiableMap(merged);
  }

  boolean hasDeny(Set<String> privileges) {
    if (!readable) {
      return true;
    }
    return privileges.stream().anyMatch(deniedPrivileges::contains);
  }
}
