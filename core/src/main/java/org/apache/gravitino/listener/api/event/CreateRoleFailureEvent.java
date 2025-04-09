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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * Represents an event triggered when an attempt to create a role in a metalake fails due to an
 * exception.
 */
@DeveloperApi
public class CreateRoleFailureEvent extends RoleFailureEvent {
  private final String roleName;
  private final Map<String, String> properties;
  private final List<SecurableObject> securableObjects;

  /**
   * Constructs a new {@code CreateRoleFailureEvent} instance.
   *
   * @param initiator the user who initiated the event.
   * @param metalake the target metalake context for the role creation
   * @param exception the exception that caused the failure
   * @param roleName the name of the role attempted to be created
   * @param properties the properties associated with the role; if {@code null}, an empty map is
   *     used
   * @param securableObjects the list of securable objects associated with the role; if {@code
   *     null}, an empty list is used
   */
  protected CreateRoleFailureEvent(
      String initiator,
      String metalake,
      Exception exception,
      String roleName,
      Map<String, String> properties,
      List<SecurableObject> securableObjects) {
    super(initiator, NameIdentifierUtil.ofRole(metalake, roleName), exception);

    this.roleName = roleName;
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
    this.securableObjects =
        securableObjects == null ? ImmutableList.of() : ImmutableList.copyOf(securableObjects);
  }

  /**
   * Returns the name of the role that failed to be created.
   *
   * @return the name of the role
   */
  public String roleName() {
    return roleName;
  }

  /**
   * Returns the properties associated with the role.
   *
   * @return an immutable map of role properties
   */
  protected Map<String, String> properties() {
    return properties;
  }

  /**
   * Returns the list of securable objects associated with the role.
   *
   * @return an immutable list of securable objects
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
