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

import java.util.LinkedHashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Parses the value of the {@link AuthConstants#X_GRAVITINO_ACTIVE_ROLES_HEADER} header into an
 * {@link ActiveRoles} declaration.
 *
 * <p>Accepted grammar:
 *
 * <ul>
 *   <li>absent, empty, or whitespace-only value &rarr; {@link ActiveRoles#all()} (today's
 *       behavior);
 *   <li>{@link #ALL_KEYWORD} &rarr; {@link ActiveRoles#all()};
 *   <li>{@link #NONE_KEYWORD} &rarr; {@link ActiveRoles#none()};
 *   <li>a role name or a comma-separated list of role names &rarr; {@link ActiveRoles#of}.
 * </ul>
 *
 * <p>Entries are trimmed of surrounding whitespace and duplicates collapse; role names are matched
 * case-sensitively. The keywords {@code ALL} and {@code NONE} are reserved, matched exactly (upper
 * case), and each must appear on its own. A role whose name is exactly {@code ALL} or {@code NONE}
 * therefore cannot be activated by name.
 *
 * <p>Only <em>syntactic</em> validation is performed here. A well-formed value that names a role
 * the caller does not actually hold is not rejected by this parser; that membership check is done
 * later on the server against the caller's effective roles.
 */
public final class ActiveRoleParser {

  /** The reserved keyword that activates every role the caller holds. */
  public static final String ALL_KEYWORD = "ALL";

  /** The reserved keyword that activates no role. */
  public static final String NONE_KEYWORD = "NONE";

  private ActiveRoleParser() {}

  /**
   * Parses a raw header value into an {@link ActiveRoles} declaration.
   *
   * @param rawValue the raw header value, or {@code null} when the header is absent
   * @return the parsed declaration
   * @throws IllegalActiveRolesException if the value is syntactically invalid (an empty entry, or a
   *     reserved keyword combined with any other value)
   */
  public static ActiveRoles parse(@Nullable String rawValue) {
    if (rawValue == null) {
      return ActiveRoles.all();
    }
    String trimmed = rawValue.trim();
    if (trimmed.isEmpty()) {
      return ActiveRoles.all();
    }

    // Split with a negative limit so trailing empty entries (e.g. "analyst,") are retained and
    // therefore rejected below rather than silently dropped.
    String[] parts = trimmed.split(",", -1);
    Set<String> names = new LinkedHashSet<>();
    boolean sawAll = false;
    boolean sawNone = false;
    for (String part : parts) {
      String token = part.trim();
      if (token.isEmpty()) {
        throw new IllegalActiveRolesException(
            "Invalid '"
                + AuthConstants.X_GRAVITINO_ACTIVE_ROLES_HEADER
                + "' header: empty role entry in value '"
                + rawValue
                + "'");
      }
      if (ALL_KEYWORD.equals(token)) {
        sawAll = true;
      } else if (NONE_KEYWORD.equals(token)) {
        sawNone = true;
      } else {
        names.add(token);
      }
    }

    // A reserved keyword must appear on its own — never combined with a role name, another keyword,
    // or even a repeat of itself. Every entry is non-empty at this point, so a lone keyword is the
    // only case where a reserved word is valid.
    boolean sawReserved = sawAll || sawNone;
    if (sawReserved && parts.length > 1) {
      throw new IllegalActiveRolesException(
          "Invalid '"
              + AuthConstants.X_GRAVITINO_ACTIVE_ROLES_HEADER
              + "' header: reserved keyword "
              + ALL_KEYWORD
              + "/"
              + NONE_KEYWORD
              + " must appear alone in value '"
              + rawValue
              + "'");
    }

    if (sawAll) {
      return ActiveRoles.all();
    }
    if (sawNone) {
      return ActiveRoles.none();
    }
    return ActiveRoles.of(names);
  }
}
