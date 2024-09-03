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
import org.apache.gravitino.messaging.TopicChange;

/**
 * Represents an event that is triggered when an attempt to alter a topic fails due to an exception.
 */
@DeveloperApi
public final class AlterTopicFailureEvent extends TopicFailureEvent {
  private final TopicChange[] topicChanges;

  /**
   * Constructs an {@code AlterTopicFailureEvent} instance, capturing detailed information about the
   * failed topic alteration attempt.
   *
   * @param user The user who initiated the topic alteration operation.
   * @param identifier The identifier of the topic that was attempted to be altered.
   * @param exception The exception that was thrown during the topic alteration operation.
   * @param topicChanges The changes that were attempted on the topic.
   */
  public AlterTopicFailureEvent(
      String user, NameIdentifier identifier, Exception exception, TopicChange[] topicChanges) {
    super(user, identifier, exception);
    this.topicChanges = topicChanges.clone();
  }

  /**
   * Retrieves the changes that were attempted on the topic.
   *
   * @return An array of {@link TopicChange} objects representing the attempted modifications to the
   *     topic.
   */
  public TopicChange[] topicChanges() {
    return topicChanges;
  }
}
