/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
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

/** Represents one group to add in a bulk operation. */
public final class GroupAdd {

  private final String name;
  @Nullable private final String externalId;

  /**
   * Creates a group add item.
   *
   * @param name The group name.
   * @param externalId The external identifier, or null if unset.
   */
  public GroupAdd(String name, @Nullable String externalId) {
    this.name = name;
    this.externalId = externalId;
  }

  /**
   * Returns the group name.
   *
   * @return The group name.
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
   * Returns whether the group has an external identifier.
   *
   * @return True if the group has a non-blank external identifier, otherwise false.
   */
  public boolean hasExternalId() {
    return StringUtils.isNotBlank(externalId);
  }
}
