/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.meta.TopicEntity;

public class TopicEntitySerDe implements ProtoSerDe<TopicEntity, Topic> {

  @Override
  public Topic serialize(TopicEntity topicEntity) {
    Topic.Builder builder =
        Topic.newBuilder()
            .setId(topicEntity.id())
            .setName(topicEntity.name())
            .setAuditInfo(new AuditInfoSerDe().serialize(topicEntity.auditInfo()));

    if (topicEntity.comment() != null) {
      builder.setComment(topicEntity.comment());
    }

    if (topicEntity.properties() != null && !topicEntity.properties().isEmpty()) {
      builder.putAllProperties(topicEntity.properties());
    }

    return builder.build();
  }

  @Override
  public TopicEntity deserialize(Topic p) {
    TopicEntity.Builder builder =
        TopicEntity.builder()
            .withId(p.getId())
            .withName(p.getName())
            .withAuditInfo(new AuditInfoSerDe().deserialize(p.getAuditInfo()));

    if (p.hasComment()) {
      builder.withComment(p.getComment());
    }

    if (p.getPropertiesCount() > 0) {
      builder.withProperties(p.getPropertiesMap());
    }

    return builder.build();
  }
}
