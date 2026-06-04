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
public class ListRoleNamesEvent extends RoleEvent implements ListEvent {
  private final Optional<MetadataObject> object;
  private final int roleCount;

  /**
   * Constructs a new {@code ListRoleNamesEvent} instance with the specified metadata object and
   * count.
   *
   * @param initiator the user who triggered the event.
   * @param metalake the metalake name where the roles are listed.
   * @param object the {@code MetadataObject} related to the role names.
   * @param roleCount the number of role names returned by the list operation.
   */
  public ListRoleNamesEvent(
      String initiator, String metalake, MetadataObject object, int roleCount) {
    super(initiator, NameIdentifierUtil.ofMetalake(metalake));
    this.object = Optional.ofNullable(object);
    this.roleCount = roleCount;
  }

  /**
   * Constructs a new {@code ListRoleNamesEvent} instance without a related metadata object.
   *
   * @param initiator the user who triggered the event.
   * @param metalake the metalake name where the roles are listed.
   * @param roleCount the number of role names returned by the list operation.
   */
  public ListRoleNamesEvent(String initiator, String metalake, int roleCount) {
    this(initiator, metalake, null, roleCount);
  }

  /**
   * Constructs a new {@code ListRoleNamesEvent} instance without a related metadata object or
   * count.
   *
   * @param initiator the user who triggered the event.
   * @param metalake the metalake name where the roles are listed.
   * @deprecated Use {@link #ListRoleNamesEvent(String, String, int)} instead.
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public ListRoleNamesEvent(String initiator, String metalake) {
    this(initiator, metalake, null, -1);
  }

  /**
   * Constructs a new {@code ListRoleNamesEvent} instance with the specified metadata object but no
   * count.
   *
   * @param initiator the user who triggered the event.
   * @param metalake the metalake name where the roles are listed.
   * @param object the {@code MetadataObject} related to the role names.
   * @deprecated Use {@link #ListRoleNamesEvent(String, String, MetadataObject, int)} instead.
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public ListRoleNamesEvent(String initiator, String metalake, MetadataObject object) {
    this(initiator, metalake, object, -1);
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

  /** {@inheritDoc} */
  @Override
  public int resultCount() {
    return roleCount;
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
