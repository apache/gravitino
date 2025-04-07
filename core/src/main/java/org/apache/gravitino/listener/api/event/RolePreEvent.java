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
 * Represents a pre-event for a role operation request. This class serves as a base class for events
 * triggered before a role operation takes place.
 */
@DeveloperApi
public abstract class RolePreEvent extends PreEvent {

  /**
   * Constructs a new {@link RolePreEvent} instance with the specified initiator and identifier for
   * a role operation.
   *
   * @param initiator The user who triggered the event.
   * @param identifier The identifier of the metalake being operated on.
   */
  protected RolePreEvent(String initiator, NameIdentifier identifier) {
    super(initiator, identifier);
  }
}
