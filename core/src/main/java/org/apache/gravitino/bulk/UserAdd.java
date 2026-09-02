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
package org.apache.gravitino.bulk;

import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/** Represents one user to add in a bulk operation. */
public final class UserAdd {

  private final String name;
  @Nullable private final String externalId;
  @Nullable private final Boolean enabled;

  /**
   * Creates a user add item.
   *
   * @param name The user name.
   * @param externalId The external identifier, or null if unset.
   * @param enabled Whether the user is enabled, or null to use the default value.
   */
  public UserAdd(String name, @Nullable String externalId, @Nullable Boolean enabled) {
    this.name = name;
    this.externalId = externalId;
    this.enabled = enabled;
  }

  /**
   * Returns the user name.
   *
   * @return The user name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the external identifier.
   *
   * @return The external identifier, or null if unset.
   */
  @Nullable
  public String externalId() {
    return externalId;
  }

  /**
   * Returns whether the user has an external identifier.
   *
   * @return True if the user has an external identifier, otherwise false.
   */
  public boolean hasExternalId() {
    return StringUtils.isNotBlank(externalId);
  }

  /**
   * Returns whether the user is enabled.
   *
   * @return Whether the user is enabled, or null to use the default value.
   */
  @Nullable
  public Boolean enabled() {
    return enabled;
  }
}
