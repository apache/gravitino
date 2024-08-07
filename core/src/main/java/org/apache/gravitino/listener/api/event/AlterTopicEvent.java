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
import org.apache.gravitino.messaging.TopicChange;

/** Represents an event fired when a topic is successfully altered. */
@DeveloperApi
public final class AlterTopicEvent extends TopicEvent {
  private final TopicInfo updatedTopicInfo;
  private final TopicChange[] topicChanges;

  /**
   * Constructs an instance of {@code AlterTopicEvent}, encapsulating the key details about the
   * successful alteration of a topic.
   *
   * @param user The username of the individual responsible for initiating the topic alteration.
   * @param identifier The unique identifier of the altered topic, serving as a clear reference
   *     point for the topic in question.
   * @param topicChanges An array of {@link TopicChange} objects representing the specific changes
   *     applied to the topic during the alteration process.
   * @param updatedTopicInfo The post-alteration state of the topic.
   */
  public AlterTopicEvent(
      String user,
      NameIdentifier identifier,
      TopicChange[] topicChanges,
      TopicInfo updatedTopicInfo) {
    super(user, identifier);
    this.topicChanges = topicChanges.clone();
    this.updatedTopicInfo = updatedTopicInfo;
  }

  /**
   * Retrieves the updated state of the topic after the successful alteration.
   *
   * @return A {@link TopicInfo} instance encapsulating the details of the altered topic.
   */
  public TopicInfo updatedTopicInfo() {
    return updatedTopicInfo;
  }

  /**
   * Retrieves the specific changes that were made to the topic during the alteration process.
   *
   * @return An array of {@link TopicChange} objects detailing each modification applied to the
   *     topic.
   */
  public TopicChange[] topicChanges() {
    return topicChanges;
  }
}
