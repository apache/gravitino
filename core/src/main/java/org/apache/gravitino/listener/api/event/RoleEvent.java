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

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Represents an abstract base class for events related to role operations. This class extends
 * {@link Event} to provide a specific context for operations performed by roles. It captures
 * essential information including the user performing the operation and the identifier of the role.
 *
 * <p>Concrete implementations of this class should provide additional details relevant to the
 * specific type of role operation.
 */
@DeveloperApi
public abstract class RoleEvent extends Event {
  /**
   * Constructs a new {@code RoleEvent} instance with the given initiator and identifier.
   *
   * @param initiator the user who triggered the event.
   * @param identifier the identifier of the role affected by the event.
   */
  protected RoleEvent(String initiator, NameIdentifier identifier) {
    super(initiator, identifier);
  }

  /** {@inheritDoc} */
  @Override
  public OperationStatus operationStatus() {
    return OperationStatus.SUCCESS;
  }
}
