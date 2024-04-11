/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.messaging.Topic;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.TopicEntity;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A Topic class to represent a topic metadata object that combines the metadata both from {@link
 * Topic} and {@link TopicEntity}.
 */
public class EntityCombinedTopic implements Topic {

  private final Topic topic;
  private final TopicEntity topicEntity;

  // Sets of properties that should be hidden from the user.
  private Set<String> hiddenProperties;

  private EntityCombinedTopic(Topic topic, TopicEntity topicEntity) {
    this.topic = topic;
    this.topicEntity = topicEntity;
  }

  public static EntityCombinedTopic of(Topic topic, TopicEntity topicEntity) {
    return new EntityCombinedTopic(topic, topicEntity);
  }

  public static EntityCombinedTopic of(Topic topic) {
    return new EntityCombinedTopic(topic, null);
  }

  public EntityCombinedTopic withHiddenPropertiesSet(Set<String> hiddenProperties) {
    this.hiddenProperties = hiddenProperties;
    return this;
  }

  @Override
  public String name() {
    return topic.name();
  }

  @Override
  public String comment() {
    return topicEntity == null ? topic.comment() : topicEntity.comment();
  }

  @Override
  public Map<String, String> properties() {
    return topic.properties().entrySet().stream()
        .filter(p -> !hiddenProperties.contains(p.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public Audit auditInfo() {
    AuditInfo mergedAudit =
        AuditInfo.builder()
            .withCreator(topic.auditInfo().creator())
            .withCreateTime(topic.auditInfo().createTime())
            .withLastModifier(topic.auditInfo().lastModifier())
            .withLastModifiedTime(topic.auditInfo().lastModifiedTime())
            .build();

    return topicEntity == null
        ? topic.auditInfo()
        : mergedAudit.merge(topicEntity.auditInfo(), true /* overwrite */);
  }
}
