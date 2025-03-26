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

package org.apache.gravitino.listener.api.event;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.authorization.SecurableObject;

/** Represents an event that is generated before a role is successfully created. */
@DeveloperApi
public class CreateRolePreEvent extends RolePreEvent {
  private final String roleName;
  private final Map<String, String> properties;
  private final List<SecurableObject> securableObjects;

  /**
   * Constructs a new {@link CreateRolePreEvent} instance with the specified initiator, identifier,
   * role, properties, and securable objects.
   *
   * @param initiator the user who initiated the event.
   * @param identifier the identifier of the metalake which is being operated on.
   * @param roleName the name of the role being created.
   * @param properties the properties of the role being created.
   * @param securableObjects the list of securable objects which belong to the role.
   */
  protected CreateRolePreEvent(
      String initiator,
      NameIdentifier identifier,
      String roleName,
      Map<String, String> properties,
      List<SecurableObject> securableObjects) {
    super(initiator, identifier);

    this.roleName = roleName;
    this.properties = properties;
    this.securableObjects = securableObjects;
  }

  /**
   * Returns the name of the role being created.
   *
   * @return the name of the role being created.
   */
  public String roleName() {
    return roleName;
  }

  /**
   * Returns the properties of the role being created.
   *
   * @return the properties of the role being created.
   */
  protected Map<String, String> properties() {
    return properties;
  }

  /**
   * Returns the list of securable objects which belong to the role.
   *
   * @return the list of securable objects which belong to the role.
   */
  protected List<SecurableObject> securableObjects() {
    return securableObjects;
  }

  /**
   * Returns the operation type of this event.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.CREATE_ROLE;
  }
}
