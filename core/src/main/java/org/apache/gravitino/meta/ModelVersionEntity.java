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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.ToString;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;

@ToString
public class ModelVersionEntity implements Entity, Auditable {

  public static final Field VERSION =
      Field.required("version", Integer.class, "The version of the model entity.");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment or description of the model entity.");
  public static final Field ALIASES =
      Field.optional("aliases", List.class, "The aliases of the model entity.");
  public static final Field URL =
      Field.required("uri", String.class, "The URI of the model entity.");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the model entity.");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the model entity.");

  private Integer version;

  private String comment;

  private List<String> aliases;

  private String uri;

  private AuditInfo auditInfo;

  private Map<String, String> properties;

  private ModelVersionEntity() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(VERSION, version);
    fields.put(COMMENT, comment);
    fields.put(ALIASES, aliases);
    fields.put(URL, uri);
    fields.put(PROPERTIES, properties);
    fields.put(AUDIT_INFO, auditInfo);

    return Collections.unmodifiableMap(fields);
  }

  public Integer version() {
    return version;
  }

  public String comment() {
    return comment;
  }

  public List<String> aliases() {
    return aliases;
  }

  public String uri() {
    return uri;
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
    return EntityType.MODEL_VERSION;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof ModelVersionEntity)) {
      return false;
    }

    ModelVersionEntity that = (ModelVersionEntity) o;
    return Objects.equals(version, that.version)
        && Objects.equals(comment, that.comment)
        && Objects.equals(aliases, that.aliases)
        && Objects.equals(uri, that.uri)
        && Objects.equals(properties, that.properties)
        && Objects.equals(auditInfo, that.auditInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, comment, aliases, uri, properties, auditInfo);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final ModelVersionEntity model;

    private Builder() {
      model = new ModelVersionEntity();
    }

    public Builder withVersion(int version) {
      model.version = version;
      return this;
    }

    public Builder withComment(String comment) {
      model.comment = comment;
      return this;
    }

    public Builder withAliases(List<String> aliases) {
      model.aliases = aliases;
      return this;
    }

    public Builder withUri(String uri) {
      model.uri = uri;
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

    public ModelVersionEntity build() {
      model.validate();
      return model;
    }
  }
}
