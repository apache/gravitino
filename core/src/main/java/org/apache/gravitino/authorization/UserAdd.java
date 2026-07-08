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
package org.apache.gravitino.authorization;

import javax.annotation.Nullable;

/** Represents a User to add. */
public class UserAdd {

  private final String name;
  @Nullable private final String externalId;
  @Nullable private final Boolean enabled;

  /**
   * Creates a new UserAdd instance.
   *
   * @param name The name of the User.
   * @param externalId The external identifier of the User.
   * @param enabled Whether the User is enabled.
   */
  public UserAdd(String name, @Nullable String externalId, @Nullable Boolean enabled) {
    this.name = name;
    this.externalId = externalId;
    this.enabled = enabled;
  }

  /**
   * Gets the name of the User.
   *
   * @return The name of the User.
   */
  public String name() {
    return name;
  }

  /**
   * Gets the external identifier of the User.
   *
   * @return The external identifier of the User.
   */
  @Nullable
  public String externalId() {
    return externalId;
  }

  /**
   * Gets whether the User is enabled.
   *
   * @return Whether the User is enabled.
   */
  @Nullable
  public Boolean enabled() {
    return enabled;
  }
}
