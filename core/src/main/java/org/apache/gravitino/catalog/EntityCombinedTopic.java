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
package org.apache.gravitino.catalog;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Audit;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.connector.HiddenPropertyMaskUtils;
import org.apache.gravitino.connector.HiddenPropertyMaskUtils.PropertyResponsePolicy;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TopicEntity;

/**
 * A Topic class to represent a topic metadata object that combines the metadata both from {@link
 * Topic} and {@link TopicEntity}.
 */
public class EntityCombinedTopic implements Topic {

  private final Topic topic;
  private final TopicEntity topicEntity;

  // Property keys whose values are masked in API responses.
  private Set<String> hiddenProperties = Collections.emptySet();

  // Reserved+hidden system keys omitted from API responses (e.g. gravitino.identifier).
  private Set<String> omittedProperties = Collections.emptySet();

  // Field "imported" is used to indicate whether the entity has been imported to Gravitino
  // managed storage backend. If "imported" is true, it means that storage backend have stored
  // the correct entity. Otherwise, we should import the external entity to the storage backend.
  // This is used for tag/access control related purposes, only the imported entities have the
  // unique id, and based on this id, we can label and control the access to the entities.
  private boolean imported;

  private EntityCombinedTopic(Topic topic, TopicEntity topicEntity) {
    this.topic = topic;
    this.topicEntity = topicEntity;
    this.imported = false;
  }

  public TopicEntity topicEntity() {
    return topicEntity;
  }

  public Topic topic() {
    return topic;
  }

  public static EntityCombinedTopic of(Topic topic, TopicEntity topicEntity) {
    return new EntityCombinedTopic(topic, topicEntity);
  }

  public static EntityCombinedTopic of(Topic topic) {
    return new EntityCombinedTopic(topic, null);
  }

  public EntityCombinedTopic withHiddenProperties(Set<String> hiddenProperties) {
    this.hiddenProperties = hiddenProperties == null ? Collections.emptySet() : hiddenProperties;
    this.omittedProperties = Collections.emptySet();
    return this;
  }

  /** Applies mask/omit policy for API property responses. */
  public EntityCombinedTopic withHiddenProperties(PropertyResponsePolicy policy) {
    if (policy == null) {
      this.hiddenProperties = Collections.emptySet();
      this.omittedProperties = Collections.emptySet();
    } else {
      this.hiddenProperties = policy.keysToMask();
      this.omittedProperties = policy.keysToOmit();
    }
    return this;
  }

  public EntityCombinedTopic withImported(boolean imported) {
    this.imported = imported;
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
    return HiddenPropertyMaskUtils.maskHiddenProperties(
        topic.properties(), hiddenProperties, omittedProperties);
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

  public boolean imported() {
    return imported;
  }

  StringIdentifier stringIdentifier() {
    return StringIdentifier.fromProperties(topic.properties());
  }
}
