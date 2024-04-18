/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.info;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.messaging.Topic;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;

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
