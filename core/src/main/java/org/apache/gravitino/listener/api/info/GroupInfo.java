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

package org.apache.gravitino.listener.api.info;

import java.util.List;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.authorization.Group;

/** Provides read-only access to group information for event listeners. */
@DeveloperApi
public class GroupInfo {
  private final String name;
  private List<String> roles;

  /**
   * Constructs a new {@link GroupInfo} instance from the specified {@link Group} object.
   *
   * @param group the {@link Group} object from which to create the {@link GroupInfo}.
   */
  public GroupInfo(Group group) {
    this.name = group.name();
    this.roles = group.roles();
  }

  /**
   * Returns the name of the group.
   *
   * @return The name of the group.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the roles of the roles.
   *
   * @return The roles of the group.
   */
  public List<String> roles() {
    return roles;
  }
}
