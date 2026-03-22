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

package org.apache.gravitino;

import com.google.common.base.Preconditions;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** A simple implementation of Principal that holds a username. */
public class UserPrincipal implements Principal {

  private final String username;
  private final List<String> groups;

  /**
   * Constructs a UserPrincipal with the given username.
   *
   * @param username the username of the principal
   */
  public UserPrincipal(final String username) {
    this(username, Collections.emptyList());
  }

  /**
   * Constructs a UserPrincipal with the given username and groups.
   *
   * @param username the username of the principal
   * @param groups the groups of the principal
   */
  public UserPrincipal(final String username, final List<String> groups) {
    Preconditions.checkArgument(username != null, "UserPrincipal must have the username");
    this.username = username;
    this.groups =
        groups != null
            ? Collections.unmodifiableList(new ArrayList<>(groups))
            : Collections.emptyList();
  }

  /**
   * Returns the username of this principal.
   *
   * @return the username
   */
  @Override
  public String getName() {
    return username;
  }

  /**
   * Returns the groups of this principal.
   *
   * @return the groups
   */
  public List<String> getGroups() {
    return groups;
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, groups);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UserPrincipal)) {
      return false;
    }
    UserPrincipal that = (UserPrincipal) o;
    return Objects.equals(username, that.username) && Objects.equals(groups, that.groups);
  }

  @Override
  public String toString() {
    return "[principal: " + this.username + ", groups: " + this.groups + "]";
  }
}
