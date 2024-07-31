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
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.DeveloperApi;

/** Represents an event that is triggered upon the successful list of topics within a namespace. */
@DeveloperApi
public final class ListTopicEvent extends TopicEvent {
  private final Namespace namespace;

  /**
   * Constructs an instance of {@code ListTopicEvent}.
   *
   * @param user The username of the individual who initiated the topic listing.
   * @param namespace The namespace from which topics were listed.
   */
  public ListTopicEvent(String user, Namespace namespace) {
    super(user, NameIdentifier.of(namespace.levels()));
    this.namespace = namespace;
  }

  /**
   * Provides the namespace associated with this event.
   *
   * @return A {@link Namespace} instance from which topics were listed.
   */
  public Namespace namespace() {
    return namespace;
  }
}
