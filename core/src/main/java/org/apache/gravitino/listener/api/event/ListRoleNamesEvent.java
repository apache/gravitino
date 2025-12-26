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

/** Represents an event triggered after role names are listed. */
@DeveloperApi
public class ListRoleNamesEvent extends RoleEvent {
  private final Optional<MetadataObject> object;

  /**
   * Constructs a new {@code ListRoleNamesEvent} instance without a related metadata object.
   *
   * @param initiator the user who triggered the event.
   * @param metalake the metalake name where the roles are listed.
   */
  public ListRoleNamesEvent(String initiator, String metalake) {
    this(initiator, metalake, null);
  }

  /**
   * Constructs a new {@code ListRoleNamesEvent} instance with the specified metadata object.
   *
   * @param initiator the user who triggered the event.
   * @param metalake the metalake name where the roles are listed.
   * @param object the {@code MetadataObject} related to the role names.
   */
  public ListRoleNamesEvent(String initiator, String metalake, MetadataObject object) {
    super(initiator, NameIdentifierUtil.ofMetalake(metalake));

    this.object = Optional.ofNullable(object);
  }

  /**
   * Returns the {@code MetadataObject} related to the role names.
   *
   * @return an {@code Optional} containing the {@code MetadataObject} if present, otherwise an
   *     empty {@code Optional}.
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
