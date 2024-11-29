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

@ToString
public class ModelEntity implements Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, "The unique id of the model entity.");
  public static final Field NAME =
      Field.required("name", String.class, "The name of the model entity.");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment or description of the model entity.");
  public static final Field LATEST_VERSION =
      Field.required("latest_version", Integer.class, "The latest version of the model entity.");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the model entity.");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the model entity.");

  private Long id;

  private String name;

  private Namespace namespace;

  private String comment;

  private Integer latestVersion;

  private AuditInfo auditInfo;

  private Map<String, String> properties;

  private ModelEntity() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(LATEST_VERSION, latestVersion);
    fields.put(PROPERTIES, properties);
    fields.put(AUDIT_INFO, auditInfo);

    return Collections.unmodifiableMap(fields);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Long id() {
    return id;
  }

  @Override
  public Namespace namespace() {
    return namespace;
  }

  public String comment() {
    return comment;
  }

  public Integer latestVersion() {
    return latestVersion;
  }

  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  @Override
  public EntityType type() {
    return EntityType.MODEL;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof ModelEntity)) {
      return false;
    }

    ModelEntity that = (ModelEntity) o;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && Objects.equals(comment, that.comment)
        && Objects.equals(latestVersion, that.latestVersion)
        && Objects.equals(properties, that.properties)
        && Objects.equals(auditInfo, that.auditInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, comment, latestVersion, properties, auditInfo);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final ModelEntity model;

    private Builder() {
      model = new ModelEntity();
    }

    public Builder withId(Long id) {
      model.id = id;
      return this;
    }

    public Builder withName(String name) {
      model.name = name;
      return this;
    }

    public Builder withNamespace(Namespace namespace) {
      model.namespace = namespace;
      return this;
    }

    public Builder withComment(String comment) {
      model.comment = comment;
      return this;
    }

    public Builder withLatestVersion(Integer latestVersion) {
      model.latestVersion = latestVersion;
      return this;
    }

    public Builder withProperties(Map<String, String> properties) {
      model.properties = properties;
      return this;
    }

    public Builder withAuditInfo(AuditInfo auditInfo) {
      model.auditInfo = auditInfo;
      return this;
    }

    public ModelEntity build() {
      model.validate();
      return model;
    }
  }
}
