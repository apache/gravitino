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

import java.util.Optional;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/** Represents an event that is generated before listing the role names of a metalake. */
@DeveloperApi
public class ListRoleNamesPreEvent extends RolePreEvent {
  private final Optional<MetadataObject> object;

  /**
   * Constructs a new {@link ListRoleNamesPreEvent} instance with the given initiator and
   * identifier.
   *
   * @param initiator the user who initiated the event.
   * @param identifier the identifier of the metalake which is being operated on.
   */
  public ListRoleNamesPreEvent(String initiator, NameIdentifier identifier) {
    this(initiator, identifier, null);
  }

  /**
   * Constructs a new {@link ListRoleNamesPreEvent} instance with the given initiator, identifier
   * and {@link MetadataObject} instance.
   *
   * @param initiator the user who initiated the event.
   * @param identifier the identifier of the metalake which is being operated on.
   * @param object the {@link MetadataObject} instance.
   */
  public ListRoleNamesPreEvent(String initiator, NameIdentifier identifier, MetadataObject object) {
    super(initiator, identifier);

    this.object = Optional.ofNullable(object);
  }

  /**
   * Returns the {@link MetadataObject} instance of the role.
   *
   * @return the {@link MetadataObject} instance of the role.
   */
  public Optional<MetadataObject> object() {
    return object;
  }

  /**
   * Returns the operation type of this event.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.LIST_ROLE_NAMES;
  }
}
