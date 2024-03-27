/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.kafka;

import static com.datastrato.gravitino.catalog.kafka.KafkaTopicPropertiesMetadata.PARTITION_COUNT;

import com.datastrato.gravitino.connector.BaseTopic;
import java.util.Optional;
import org.apache.kafka.clients.admin.NewTopic;

public class KafkaTopic extends BaseTopic {

  public NewTopic toKafkaTopic(KafkaTopicPropertiesMetadata propertiesMetadata) {
    Optional<Integer> partitionCount =
        Optional.ofNullable((int) propertiesMetadata.getOrDefault(properties(), PARTITION_COUNT));
    Optional<Short> replicationFactor =
        Optional.ofNullable(
            (short)
                propertiesMetadata.getOrDefault(
                    properties(), KafkaTopicPropertiesMetadata.REPLICATION_FACTOR));
    return new NewTopic(name, partitionCount, replicationFactor);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends BaseTopicBuilder<Builder, KafkaTopic> {

    @Override
    protected KafkaTopic internalBuild() {
      KafkaTopic topic = new KafkaTopic();
      topic.name = name;
      topic.comment = comment;
      topic.properties = properties;
      topic.auditInfo = auditInfo;
      return topic;
    }
  }
}
