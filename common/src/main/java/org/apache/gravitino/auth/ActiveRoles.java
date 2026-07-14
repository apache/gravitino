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

package org.apache.gravitino.auth;

import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * The set of roles a caller has declared active for a request (role assumption).
 *
 * <p>An instance is one of three kinds:
 *
 * <ul>
 *   <li>{@link Mode#ALL} — every role the caller holds is active (identical to today's behavior and
 *       to an absent header);
 *   <li>{@link Mode#NONE} — no role is active; all role-derived access is removed;
 *   <li>{@link Mode#NAMED} — only the listed role names are active.
 * </ul>
 *
 * <p>This type only models the declared value. Validating that the caller actually holds the named
 * roles happens later, on the server, against the caller's effective roles.
 */
public final class ActiveRoles {

  /** The kind of active-role declaration. */
  public enum Mode {
    /** Every role the caller holds is active. */
    ALL,
    /** No role is active. */
    NONE,
    /** Only the explicitly named roles are active. */
    NAMED
  }

  private static final ActiveRoles ALL = new ActiveRoles(Mode.ALL, Collections.emptySet());
  private static final ActiveRoles NONE = new ActiveRoles(Mode.NONE, Collections.emptySet());

  private final Mode mode;
  private final Set<String> roleNames;

  private ActiveRoles(Mode mode, Set<String> roleNames) {
    this.mode = mode;
    this.roleNames = roleNames;
  }

  /**
   * Returns the {@link Mode#ALL} declaration: every role the caller holds is active.
   *
   * @return the {@code ALL} declaration
   */
  public static ActiveRoles all() {
    return ALL;
  }

  /**
   * Returns the {@link Mode#NONE} declaration: no role is active.
   *
   * @return the {@code NONE} declaration
   */
  public static ActiveRoles none() {
    return NONE;
  }

  /**
   * Returns a {@link Mode#NAMED} declaration for the given role names. Names are de-duplicated
   * while preserving their first-seen order; role names are treated case-sensitively.
   *
   * @param roleNames the active role names; must be non-empty and contain no blank name
   * @return a {@code NAMED} declaration
   * @throws IllegalArgumentException if {@code roleNames} is null, empty, or contains a blank name
   */
  public static ActiveRoles of(Collection<String> roleNames) {
    Preconditions.checkArgument(
        roleNames != null && !roleNames.isEmpty(), "Active role names must not be empty");
    Set<String> names = new LinkedHashSet<>();
    for (String name : roleNames) {
      Preconditions.checkArgument(
          name != null && !name.trim().isEmpty(), "Active role name must not be blank");
      names.add(name);
    }
    return new ActiveRoles(Mode.NAMED, Collections.unmodifiableSet(names));
  }

  /**
   * Returns the kind of this declaration.
   *
   * @return the mode
   */
  public Mode mode() {
    return mode;
  }

  /**
   * Returns the declared role names. Empty unless {@link #mode()} is {@link Mode#NAMED}.
   *
   * @return an immutable, ordered set of role names
   */
  public Set<String> roleNames() {
    return roleNames;
  }

  /**
   * Whether this declaration activates every role the caller holds.
   *
   * @return {@code true} if the mode is {@link Mode#ALL}
   */
  public boolean isAll() {
    return mode == Mode.ALL;
  }

  /**
   * Whether this declaration activates no role.
   *
   * @return {@code true} if the mode is {@link Mode#NONE}
   */
  public boolean isNone() {
    return mode == Mode.NONE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ActiveRoles)) {
      return false;
    }
    ActiveRoles that = (ActiveRoles) o;
    return mode == that.mode && roleNames.equals(that.roleNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mode, roleNames);
  }

  @Override
  public String toString() {
    if (mode == Mode.NAMED) {
      return "ActiveRoles{NAMED=" + roleNames + "}";
    }
    return "ActiveRoles{" + mode + "}";
  }
}
