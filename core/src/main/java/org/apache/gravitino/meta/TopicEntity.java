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
package org.apache.gravitino.meta;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import lombok.ToString;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;

/** A class representing a topic metadata entity in Apache Gravitino. */
@ToString
public class TopicEntity implements Entity, Auditable, HasIdentifier {
  public static final Field ID =
      Field.required("id", Long.class, "The unique id of the topic entity.");
  public static final Field NAME =
      Field.required("name", String.class, "The name of the topic entity.");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment or description of the topic entity.");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the topic entity.");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the topic entity.");

  public static Builder builder() {
    return new Builder();
  }

  private Long id;
  private String name;
  private Namespace namespace;
  private String comment;
  private AuditInfo auditInfo;
  private Map<String, String> properties;

  private TopicEntity() {}

  /**
   * Returns a map of fields and their corresponding values for this topic entity.
   *
   * @return An unmodifiable map of the fields and values.
   */
  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(AUDIT_INFO, auditInfo);
    fields.put(PROPERTIES, properties);

    return Collections.unmodifiableMap(fields);
  }

  /**
   * Returns the name of the topic.
   *
   * @return The name of the topic.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * Returns the namespace of the topic.
   *
   * @return The namespace of the topic.
   */
  @Override
  public Namespace namespace() {
    return namespace;
  }

  /**
   * Returns the unique id of the topic.
   *
   * @return The unique id of the topic.
   */
  @Override
  public Long id() {
    return id;
  }

  /**
   * Returns the comment or description of the topic.
   *
   * @return The comment or description of the topic.
   */
  public String comment() {
    return comment;
  }

  /**
   * Returns the audit details of the topic.
   *
   * @return The audit details of the topic.
   */
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /**
   * Returns the type of the entity.
   *
   * @return The type of the entity.
   */
  @Override
  public EntityType type() {
    return EntityType.TOPIC;
  }

  /**
   * Returns the properties of the topic entity.
   *
   * @return The properties of the topic entity.
   */
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TopicEntity)) return false;

    TopicEntity that = (TopicEntity) o;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(comment, that.comment)
        && Objects.equals(auditInfo, that.auditInfo)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, comment, auditInfo, properties);
  }

  public static class Builder {
    private final TopicEntity topic;

    private Builder() {
      topic = new TopicEntity();
    }

    /**
     * Sets the unique id of the topic entity.
     *
     * @param id The unique id of the topic entity.
     * @return The builder instance.
     */
    public TopicEntity.Builder withId(Long id) {
      topic.id = id;
      return this;
    }

    /**
     * Sets the name of the topic entity.
     *
     * @param name The name of the topic entity.
     * @return The builder instance.
     */
    public TopicEntity.Builder withName(String name) {
      topic.name = name;
      return this;
    }

    /**
     * Sets the namespace of the topic entity.
     *
     * @param namespace The namespace of the topic entity.
     * @return The builder instance.
     */
    public TopicEntity.Builder withNamespace(Namespace namespace) {
      topic.namespace = namespace;
      return this;
    }

    /**
     * Sets the comment or description of the topic entity.
     *
     * @param comment The comment or description of the topic entity.
     * @return The builder instance.
     */
    public TopicEntity.Builder withComment(String comment) {
      topic.comment = comment;
      return this;
    }

    /**
     * Sets the audit details of the topic entity.
     *
     * @param auditInfo The audit details of the topic entity.
     * @return The builder instance.
     */
    public TopicEntity.Builder withAuditInfo(AuditInfo auditInfo) {
      topic.auditInfo = auditInfo;
      return this;
    }

    /**
     * Sets the properties of the topic entity.
     *
     * @param properties The properties of the topic entity.
     * @return The builder instance.
     */
    public TopicEntity.Builder withProperties(Map<String, String> properties) {
      topic.properties = properties;
      return this;
    }

    /**
     * Builds the topic entity.
     *
     * @return The built topic entity.
     */
    public TopicEntity build() {
      topic.validate();
      return topic;
    }
  }
}
