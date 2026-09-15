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

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.authorization.SecurableObject;

/** Represents one role to add in a bulk operation. */
public final class RoleAdd {

  private final String name;
  @Nullable private final Map<String, String> properties;
  private final List<SecurableObject> securableObjects;

  /**
   * Creates a role add item.
   *
   * @param name The role name.
   * @param properties The role properties, or null if unset.
   * @param securableObjects The securable objects of the role.
   */
  public RoleAdd(
      String name,
      @Nullable Map<String, String> properties,
      List<SecurableObject> securableObjects) {
    this.name = name;
    this.properties = properties;
    this.securableObjects = securableObjects;
  }

  /**
   * Returns the role name.
   *
   * @return The role name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the role properties.
   *
   * @return The role properties, or null if unset.
   */
  @Nullable
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * Returns the securable objects.
   *
   * @return The securable objects of the role.
   */
  public List<SecurableObject> securableObjects() {
    return securableObjects;
  }
}
