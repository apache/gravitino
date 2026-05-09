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
import java.util.Objects;
import java.util.Optional;

/** A class representing a user group with external UID and name. */
public class UserGroup {

  private final Optional<String> groupExternalUID;
  private final String groupname;

  /**
   * Constructs a UserGroup instance.
   *
   * @param groupExternalUID The external UID of the group.
   * @param groupname The name of the group.
   */
  public UserGroup(Optional<String> groupExternalUID, String groupname) {
    Preconditions.checkArgument(groupname != null, "groupname cannot be null");
    this.groupExternalUID = groupExternalUID;
    this.groupname = groupname;
  }

  /**
   * Returns the external UID of the group.
   *
   * @return The group external UID.
   */
  public Optional<String> getGroupExternalUID() {
    return groupExternalUID;
  }

  /**
   * Returns the name of the group.
   *
   * @return The group name.
   */
  public String getGroupname() {
    return groupname;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UserGroup)) {
      return false;
    }
    UserGroup userGroup = (UserGroup) o;
    return Objects.equals(groupExternalUID, userGroup.groupExternalUID)
        && Objects.equals(groupname, userGroup.groupname);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupExternalUID, groupname);
  }

  @Override
  public String toString() {
    return "UserGroup{"
        + "groupExternalUID="
        + groupExternalUID
        + ", groupname='"
        + groupname
        + '\''
        + '}';
  }
}
