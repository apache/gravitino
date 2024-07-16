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
package org.apache.gravitino.proto;

import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.TopicEntity;

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
  public TopicEntity deserialize(Topic p, Namespace namespace) {
    TopicEntity.Builder builder =
        TopicEntity.builder()
            .withId(p.getId())
            .withName(p.getName())
            .withNamespace(namespace)
            .withAuditInfo(new AuditInfoSerDe().deserialize(p.getAuditInfo(), namespace));

    if (p.hasComment()) {
      builder.withComment(p.getComment());
    }

    if (p.getPropertiesCount() > 0) {
      builder.withProperties(p.getPropertiesMap());
    }

    return builder.build();
  }
}
