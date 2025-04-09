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
import org.apache.gravitino.authorization.User;

/** Provides read-only access to user information for event listeners. */
@DeveloperApi
public class UserInfo {
  private final String name;
  private List<String> roles;

  /**
   * Construct a new {@link UserInfo} instance with the given {@link User} information.
   *
   * @param user the {@link User} instance.
   */
  public UserInfo(User user) {
    this.name = user.name();
    this.roles = user.roles();
  }

  /**
   * Returns the name of the user.
   *
   * @return the name of the user
   */
  public String name() {
    return name;
  }

  /**
   * Returns the roles of the user.
   *
   * @return the roles of the user.
   */
  public List<String> roles() {
    return roles;
  }
}
