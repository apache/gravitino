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

import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * The abstract base class for all events. It encapsulates common information such as the user who
 * generated the event and the identifier for the resource associated with the event. Subclasses
 * should provide specific details related to their individual event types.
 */
@DeveloperApi
public abstract class BaseEvent {
  private final String user;
  @Nullable private final NameIdentifier identifier;
  private final long eventTime;

  /**
   * Constructs an Event instance with the specified user and resource identifier details.
   *
   * @param user The user associated with this event. It provides context about who triggered the
   *     event.
   * @param identifier The resource identifier associated with this event. This may refer to various
   *     types of resources such as a metalake, catalog, schema, or table, etc.
   */
  protected BaseEvent(String user, NameIdentifier identifier) {
    this.user = user;
    this.identifier = identifier;
    this.eventTime = System.currentTimeMillis();
  }

  /**
   * Retrieves the user associated with this event.
   *
   * @return A string representing the user associated with this event.
   */
  public String user() {
    return user;
  }

  /**
   * Retrieves the resource identifier associated with this event.
   *
   * <p>For list operations within a namespace, the identifier is the identifier corresponds to that
   * namespace. For metalake list operation, identifier is null.
   *
   * @return A NameIdentifier object that represents the resource, like a metalake, catalog, schema,
   *     table, etc., associated with the event.
   */
  @Nullable
  public NameIdentifier identifier() {
    return identifier;
  }

  /**
   * Returns the timestamp when the event was created.
   *
   * @return The event creation time in milliseconds since epoch.
   */
  public long eventTime() {
    return eventTime;
  }
}
