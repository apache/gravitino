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
import org.apache.gravitino.listener.api.info.TopicInfo;

/** Represents an event triggered upon the successful loading of a topic. */
@DeveloperApi
public final class LoadTopicEvent extends TopicEvent {
  private final TopicInfo loadedTopicInfo;

  /**
   * Constructs an instance of {@code LoadTopicEvent}.
   *
   * @param user The username of the individual who initiated the topic loading.
   * @param identifier The unique identifier of the topic that was loaded.
   * @param topicInfo The state of the topic post-loading.
   */
  public LoadTopicEvent(String user, NameIdentifier identifier, TopicInfo topicInfo) {
    super(user, identifier);
    this.loadedTopicInfo = topicInfo;
  }

  /**
   * Retrieves the state of the topic as it was made available to the user after successful loading.
   *
   * @return A {@link TopicInfo} instance encapsulating the details of the topic as loaded.
   */
  public TopicInfo loadedTopicInfo() {
    return loadedTopicInfo;
  }
}
