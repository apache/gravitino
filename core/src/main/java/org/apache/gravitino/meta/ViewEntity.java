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
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import lombok.ToString;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.View;

/** A class representing a view entity in the metadata store. */
@ToString
public class ViewEntity implements Entity, Auditable, HasIdentifier, View {

  public static final Field ID =
      Field.required("id", Long.class, "The unique id of the view entity.");
  public static final Field NAME =
      Field.required("name", String.class, "The name of the view entity.");
  public static final Field NAMESPACE =
      Field.required("namespace", Namespace.class, "The namespace of the view entity.");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment or description of the view entity.");
  public static final Field COLUMNS =
      Field.required("columns", Column[].class, "The output columns of the view.");
  public static final Field REPRESENTATIONS =
      Field.required("representations", Representation[].class, "The view representations.");
  public static final Field DEFAULT_CATALOG =
      Field.optional(
          "default_catalog",
          String.class,
          "The default catalog used to resolve unqualified identifiers in the view definition.");
  public static final Field DEFAULT_SCHEMA =
      Field.optional(
          "default_schema",
          String.class,
          "The default schema used to resolve unqualified identifiers in the view definition.");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the view.");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the view entity.");

  private Long id;
  private String name;
  private Namespace namespace;
  private String comment;
  private Column[] columns;
  private Representation[] representations;
  private String defaultCatalog;
  private String defaultSchema;
  private Map<String, String> properties;
  private AuditInfo auditInfo;

  private ViewEntity() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(NAMESPACE, namespace);
    fields.put(COMMENT, comment);
    fields.put(COLUMNS, columns);
    fields.put(REPRESENTATIONS, representations);
    fields.put(DEFAULT_CATALOG, defaultCatalog);
    fields.put(DEFAULT_SCHEMA, defaultSchema);
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

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Column[] columns() {
    return columns;
  }

  @Override
  public Representation[] representations() {
    return representations;
  }

  /**
   * Returns the default catalog used to resolve unqualified identifiers in the view definition.
   *
   * @return The default catalog, or {@code null} if not set.
   */
  @Override
  public String defaultCatalog() {
    return defaultCatalog;
  }

  /**
   * Returns the default schema used to resolve unqualified identifiers in the view definition.
   *
   * @return The default schema, or {@code null} if not set.
   */
  @Override
  public String defaultSchema() {
    return defaultSchema;
  }

  @Override
  public Map<String, String> properties() {
    return properties == null ? Collections.emptyMap() : properties;
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  @Override
  public EntityType type() {
    return EntityType.VIEW;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ViewEntity)) {
      return false;
    }
    ViewEntity that = (ViewEntity) o;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(comment, that.comment)
        && Arrays.equals(columns, that.columns)
        && Arrays.equals(representations, that.representations)
        && Objects.equals(defaultCatalog, that.defaultCatalog)
        && Objects.equals(defaultSchema, that.defaultSchema)
        && Objects.equals(properties, that.properties)
        && Objects.equals(auditInfo, that.auditInfo);
  }

  @Override
  public int hashCode() {
    int result =
        Objects.hash(
            id, name, namespace, comment, defaultCatalog, defaultSchema, properties, auditInfo);
    result = 31 * result + Arrays.hashCode(columns);
    result = 31 * result + Arrays.hashCode(representations);
    return result;
  }

  /**
   * Creates a new builder for constructing a ViewEntity.
   *
   * @return A new builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder class for creating instances of {@link ViewEntity}. */
  public static class Builder {
    private final ViewEntity viewEntity;

    private Builder() {
      viewEntity = new ViewEntity();
    }

    /**
     * Sets the unique id of the view entity.
     *
     * @param id The unique id.
     * @return This builder instance.
     */
    public Builder withId(Long id) {
      viewEntity.id = id;
      return this;
    }

    /**
     * Sets the name of the view entity.
     *
     * @param name The name of the view.
     * @return This builder instance.
     */
    public Builder withName(String name) {
      viewEntity.name = name;
      return this;
    }

    /**
     * Sets the namespace of the view entity.
     *
     * @param namespace The namespace.
     * @return This builder instance.
     */
    public Builder withNamespace(Namespace namespace) {
      viewEntity.namespace = namespace;
      return this;
    }

    /**
     * Sets the comment of the view entity.
     *
     * @param comment The comment or description.
     * @return This builder instance.
     */
    public Builder withComment(String comment) {
      viewEntity.comment = comment;
      return this;
    }

    /**
     * Sets the output columns of the view.
     *
     * @param columns The view columns snapshot.
     * @return This builder instance.
     */
    public Builder withColumns(Column[] columns) {
      viewEntity.columns = columns;
      return this;
    }

    /**
     * Sets the representations of the view.
     *
     * @param representations The view representations.
     * @return This builder instance.
     */
    public Builder withRepresentations(Representation[] representations) {
      viewEntity.representations = representations;
      return this;
    }

    /**
     * Sets the default catalog used to resolve unqualified identifiers referenced by the view
     * definition.
     *
     * @param defaultCatalog The default catalog, may be {@code null}.
     * @return This builder instance.
     */
    public Builder withDefaultCatalog(String defaultCatalog) {
      viewEntity.defaultCatalog = defaultCatalog;
      return this;
    }

    /**
     * Sets the default schema used to resolve unqualified identifiers referenced by the view
     * definition.
     *
     * @param defaultSchema The default schema, may be {@code null}.
     * @return This builder instance.
     */
    public Builder withDefaultSchema(String defaultSchema) {
      viewEntity.defaultSchema = defaultSchema;
      return this;
    }

    /**
     * Sets the properties of the view.
     *
     * @param properties The view properties.
     * @return This builder instance.
     */
    public Builder withProperties(Map<String, String> properties) {
      viewEntity.properties = properties;
      return this;
    }

    /**
     * Sets the audit information.
     *
     * @param auditInfo The audit information.
     * @return This builder instance.
     */
    public Builder withAuditInfo(AuditInfo auditInfo) {
      viewEntity.auditInfo = auditInfo;
      return this;
    }

    /**
     * Builds the ViewEntity instance.
     *
     * @return The constructed ViewEntity.
     */
    public ViewEntity build() {
      viewEntity.validate();
      return viewEntity;
    }
  }
}
