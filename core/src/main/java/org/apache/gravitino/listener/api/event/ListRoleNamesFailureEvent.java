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
 * Represents an event triggered when an attempt to list role names for a metalake fails due to an
 * exception.
 */
@DeveloperApi
public class ListRoleNamesFailureEvent extends RoleFailureEvent {
  private final Optional<MetadataObject> metadataObject;

  /**
   * Constructs a new {@code ListRoleNamesFailureEvent} instance with no associated {@code
   * MetadataObject}.
   *
   * @param user the user who initiated the operation
   * @param metalake the name of the metalake for which role names were being listed
   * @param exception the exception that occurred during the operation
   */
  public ListRoleNamesFailureEvent(String user, String metalake, Exception exception) {
    super(user, NameIdentifierUtil.ofMetalake(metalake), exception);

    this.metadataObject = Optional.empty();
  }

  /**
   * Construct a new {@link ListRoleNamesFailureEvent} instance with the specified arguments.
   *
   * @param user The user who initiated the event.
   * @param metalake The name of the metalake for which role names are being listed.
   * @param exception The exception that occurred while listing role names.
   * @param metadataObject The {@link MetadataObject} instance related to the role names being
   *     listed, if present.
   */
  public ListRoleNamesFailureEvent(
      String user, String metalake, Exception exception, MetadataObject metadataObject) {
    super(user, NameIdentifierUtil.ofMetalake(metalake), exception);
    this.metadataObject = Optional.of(metadataObject);
  }

  /**
   * Returns the associated {@code MetadataObject} instance.
   *
   * @return an {@code Optional} containing the {@code MetadataObject} if present, otherwise empty
   */
  public Optional<MetadataObject> metadataObject() {
    return metadataObject;
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
