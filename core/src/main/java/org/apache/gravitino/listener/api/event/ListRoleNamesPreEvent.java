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
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * Represents an event generated before listing the role names of a metalake. This class
 * encapsulates the details of the role name listing event prior to its execution.
 */
@DeveloperApi
public class ListRoleNamesPreEvent extends RolePreEvent {
  private final Optional<MetadataObject> object;

  /**
   * Constructs a new {@link ListRoleNamesPreEvent} instance with the specified initiator and
   * identifier.
   *
   * @param initiator The user who initiated the event.
   * @param metalake The name of the metalake for which role names are being listed.
   */
  public ListRoleNamesPreEvent(String initiator, String metalake) {
    super(initiator, NameIdentifierUtil.ofMetalake(metalake));

    this.object = Optional.empty();
  }

  /**
   * Constructs a new {@link ListRoleNamesPreEvent} instance with the specified initiator,
   * identifier, and {@link MetadataObject} instance.
   *
   * @param initiator The user who initiated the event.
   * @param metalake The name of the metalake for which role names are being listed.
   * @param object The {@link MetadataObject} instance related to the role names.
   */
  public ListRoleNamesPreEvent(String initiator, String metalake, MetadataObject object) {
    super(initiator, NameIdentifierUtil.ofMetalake(metalake));

    this.object = Optional.ofNullable(object);
  }

  /**
   * Returns the {@link MetadataObject} instance related to the role names.
   *
   * @return The {@link MetadataObject} instance, if present.
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
