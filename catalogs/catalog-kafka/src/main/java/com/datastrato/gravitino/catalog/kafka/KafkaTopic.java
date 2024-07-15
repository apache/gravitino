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
