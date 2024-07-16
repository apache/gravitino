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
import java.util.Optional;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.tag.Tag;

public class TagEntity implements Tag, Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, "The unique id of the tag entity.");

  public static final Field NAME =
      Field.required("name", String.class, "The name of the tag entity.");

  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the tag entity.");

  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the tag entity.");

  public static final Field AUDIT_INFO =
      Field.required("audit_info", Audit.class, "The audit details of the tag entity.");

  private Long id;
  private String name;
  private Namespace namespace;
  private String comment;
  private Map<String, String> properties;
  private Audit auditInfo;

  private TagEntity() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(PROPERTIES, properties);
    fields.put(AUDIT_INFO, auditInfo);

    return Collections.unmodifiableMap(fields);
  }

  @Override
  public EntityType type() {
    return EntityType.TAG;
  }

  @Override
  public Long id() {
    return id;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Namespace namespace() {
    return namespace;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public Optional<Boolean> inherited() {
    return Optional.empty();
  }

  @Override
  public Audit auditInfo() {
    return auditInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TagEntity)) {
      return false;
    }

    TagEntity that = (TagEntity) o;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(comment, that.comment)
        && Objects.equals(properties, that.properties)
        && Objects.equals(auditInfo, that.auditInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, namespace, comment, properties, auditInfo);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private final TagEntity tagEntity;

    private Builder() {
      this.tagEntity = new TagEntity();
    }

    public Builder withId(Long id) {
      tagEntity.id = id;
      return this;
    }

    public Builder withName(String name) {
      tagEntity.name = name;
      return this;
    }

    public Builder withNamespace(Namespace namespace) {
      tagEntity.namespace = namespace;
      return this;
    }

    public Builder withComment(String comment) {
      tagEntity.comment = comment;
      return this;
    }

    public Builder withProperties(Map<String, String> properties) {
      tagEntity.properties = properties;
      return this;
    }

    public Builder withAuditInfo(Audit auditInfo) {
      tagEntity.auditInfo = auditInfo;
      return this;
    }

    public TagEntity build() {
      tagEntity.validate();
      return tagEntity;
    }
  }
}
