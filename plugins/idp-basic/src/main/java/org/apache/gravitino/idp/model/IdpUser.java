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
package org.apache.gravitino.idp.model;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.idp.dto.IdpUserDTO;

/** Built-in IdP user. */
public class IdpUser {

  private final String name;
  private final String passwordHash;
  private final List<String> groupNames;

  /**
   * Creates a built-in IdP user without a password hash.
   *
   * @param name The username.
   * @param groupNames The group names the user belongs to.
   */
  public IdpUser(String name, List<String> groupNames) {
    this.name = name;
    this.passwordHash = null;
    this.groupNames = groupNames;
  }

  /**
   * Creates a built-in IdP user with a password hash loaded from storage.
   *
   * @param name The username.
   * @param passwordHash The password hash.
   * @param groupNames The group names the user belongs to.
   */
  public IdpUser(String name, String passwordHash, List<String> groupNames) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(passwordHash), "passwordHash must not be blank");
    this.name = name;
    this.passwordHash = passwordHash;
    this.groupNames = groupNames;
  }

  /** Returns the username. */
  public String name() {
    return name;
  }

  /**
   * Returns the password hash when loaded from storage.
   *
   * @return The password hash, or null if not available.
   */
  public String passwordHash() {
    return passwordHash;
  }

  /** Returns the group names the user belongs to. */
  public List<String> groupNames() {
    return groupNames;
  }

  /**
   * Converts this user to a REST DTO.
   *
   * @return the user DTO
   */
  public IdpUserDTO toDTO() {
    return IdpUserDTO.builder().withName(name).withGroups(groupNames).build();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof IdpUser)) {
      return false;
    }
    IdpUser that = (IdpUser) other;
    return Objects.equals(name, that.name)
        && Objects.equals(passwordHash, that.passwordHash)
        && Objects.equals(groupNames, that.groupNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, passwordHash, groupNames);
  }

  @Override
  public String toString() {
    return "IdpUser{name='" + name + "', groupNames=" + groupNames + '}';
  }
}
