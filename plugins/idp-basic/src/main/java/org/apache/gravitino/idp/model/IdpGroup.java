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

import java.util.List;
import java.util.Objects;

/** Built-in IdP group. */
public class IdpGroup {

  private final String name;
  private final List<String> usernames;

  /**
   * Creates a built-in IdP group.
   *
   * @param name The group name.
   * @param usernames The usernames in the group.
   */
  public IdpGroup(String name, List<String> usernames) {
    this.name = name;
    this.usernames = usernames;
  }

  /** Returns the group name. */
  public String name() {
    return name;
  }

  /** Returns the usernames in the group. */
  public List<String> usernames() {
    return usernames;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof IdpGroup)) {
      return false;
    }
    IdpGroup that = (IdpGroup) other;
    return Objects.equals(name, that.name) && Objects.equals(usernames, that.usernames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, usernames);
  }

  @Override
  public String toString() {
    return "IdpGroup{name='" + name + "', usernames=" + usernames + '}';
  }
}
