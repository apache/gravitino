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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.gravitino.auth.ActiveRoles;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.storage.relational.po.auth.RoleUpdatedAt;

/** Immutable effective policy indexes pinned for one request, including inactive-role denies. */
final class RequestRolePolicies implements AuthorizationRequestContext.RolePolicyView {
  private final AtomicLong generation;
  private final Set<PolicyKey> allows;
  private final Set<PolicyKey> denies;
  private final Set<String> deniedPrivileges;
  private final boolean readable;

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
    Set<PolicyKey> allow = new HashSet<>();
    Set<PolicyKey> deny = new HashSet<>();
    Set<String> denied = new HashSet<>();
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
          policies
              .getIndex()
              .forEach(
                  (key, effect) -> {
                    if (effect == Effect.DENY) {
                      deny.add(key);
                    } else if (enabled) {
                      allow.add(key);
                    }
                  });
          denied.addAll(policies.getDeniedPrivileges());
        });
    allows = Collections.unmodifiableSet(allow);
    denies = Collections.unmodifiableSet(deny);
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
    return allow ? allows.contains(key) && !denies.contains(key) : denies.contains(key);
  }

  boolean hasDeny(Set<String> privileges) {
    if (!readable) {
      return true;
    }
    return privileges.stream().anyMatch(deniedPrivileges::contains);
  }
}
