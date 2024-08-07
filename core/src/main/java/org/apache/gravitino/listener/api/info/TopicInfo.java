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

package org.apache.gravitino.listener.api.info;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.messaging.Topic;

/** Provides read-only access to topic information for event listeners. */
@DeveloperApi
public final class TopicInfo {
  private final String name;
  @Nullable private final String comment;
  private final Map<String, String> properties;
  @Nullable private final Audit audit;

  /**
   * Constructs topic information based on a given topic.
   *
   * @param topic The topic to extract information from.
   */
  public TopicInfo(Topic topic) {
    this(topic.name(), topic.comment(), topic.properties(), topic.auditInfo());
  }

  /**
   * Constructs topic information with detailed parameters.
   *
   * @param name The name of the topic.
   * @param comment An optional description of the topic.
   * @param properties A map of topic properties.
   * @param audit Optional audit information.
   */
  public TopicInfo(String name, String comment, Map<String, String> properties, Audit audit) {
    this.name = name;
    this.comment = comment;
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
    this.audit = audit;
  }

  /**
   * Gets the topic name.
   *
   * @return The topic name.
   */
  public String name() {
    return name;
  }

  /**
   * Gets the optional topic comment.
   *
   * @return The topic comment, or null if not provided.
   */
  @Nullable
  public String comment() {
    return comment;
  }

  /**
   * Gets the topic properties.
   *
   * @return An immutable map of topic properties.
   */
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * Gets the optional audit information.
   *
   * @return The audit information, or null if not provided.
   */
  @Nullable
  public Audit audit() {
    return audit;
  }
}
