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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;

/** Provides read-only access to role information for event listeners. */
@DeveloperApi
public class RoleInfo {
  private final String roleName;
  private final Map<String, String> properties;
  private final List<SecurableObject> securableObjects;

  /**
   * Constructs a new {@code RoleInfo} instance using the given {@code Role} object.
   *
   * @param roleObject the {@code Role} object providing role information.
   */
  public RoleInfo(Role roleObject) {
    this.roleName = roleObject.name();
    this.properties =
        roleObject.properties() == null
            ? ImmutableMap.of()
            : ImmutableMap.copyOf(roleObject.properties());
    this.securableObjects =
        roleObject.securableObjects() == null
            ? ImmutableList.of()
            : ImmutableList.copyOf(roleObject.securableObjects());
  }

  /**
   * Constructs a new {@code RoleInfo} instance using the given arguments.
   *
   * @param roleName The role name.
   * @param properties The properties associated with the role.
   * @param securableObjects The securable objects that belong to the role.
   */
  public RoleInfo(
      String roleName, Map<String, String> properties, List<SecurableObject> securableObjects) {
    this.roleName = roleName;
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
    this.securableObjects =
        securableObjects == null ? ImmutableList.of() : ImmutableList.copyOf(securableObjects);
  }

  /**
   * Returns the role name.
   *
   * @return the role name.
   */
  public String roleName() {
    return roleName;
  }

  /**
   * Returns the properties associated with the role.
   *
   * @return a map containing the role's properties.
   */
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * Returns the list of securable objects that belong to the role.
   *
   * @return The list of securable objects that belong to the role.
   */
  public List<SecurableObject> securableObjects() {
    return securableObjects;
  }
}
