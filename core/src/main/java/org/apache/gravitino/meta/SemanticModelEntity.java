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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import lombok.ToString;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelDefinition;

/** A metadata-store entity representing a schema-scoped Semantic Model. */
@ToString
public class SemanticModelEntity implements Entity, Auditable, HasIdentifier, SemanticModel {

  /** The unique ID field of the Semantic Model entity. */
  public static final Field ID =
      Field.required("id", Long.class, "The unique id of the Semantic Model entity.");

  /** The name field of the Semantic Model entity. */
  public static final Field NAME =
      Field.required("name", String.class, "The name of the Semantic Model entity.");

  /** The namespace field of the Semantic Model entity. */
  public static final Field NAMESPACE =
      Field.required("namespace", Namespace.class, "The namespace of the Semantic Model entity.");

  /** The optional comment field of the Semantic Model entity. */
  public static final Field COMMENT =
      Field.optional(
          "comment", String.class, "The comment or description of the Semantic Model entity.");

  /** The immutable definition field of the Semantic Model entity. */
  public static final Field DEFINITION =
      Field.required(
          "definition",
          SemanticModelDefinition.class,
          "The immutable definition of the Semantic Model entity.");

  /** The properties field of the Semantic Model entity. */
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the Semantic Model entity.");

  /** The audit information field of the Semantic Model entity. */
  public static final Field AUDIT_INFO =
      Field.required(
          "audit_info", AuditInfo.class, "The audit details of the Semantic Model entity.");

  private Long id;
  private String name;
  private Namespace namespace;
  private SemanticModelDefinition definition;
  private Map<String, String> properties = Collections.emptyMap();
  private AuditInfo auditInfo;

  @Nullable private String comment;

  private SemanticModelEntity() {}

  /**
   * Returns the fields and values of this Semantic Model entity.
   *
   * <p>The definition is immutable and the properties map is immutable, so the returned values
   * cannot mutate this entity.
   *
   * @return An unmodifiable map of fields and values.
   */
  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(NAMESPACE, namespace);
    fields.put(COMMENT, comment);
    fields.put(DEFINITION, definition);
    fields.put(PROPERTIES, properties);
    fields.put(AUDIT_INFO, auditInfo);
    return Collections.unmodifiableMap(fields);
  }

  /**
   * Returns the Semantic Model name.
   *
   * @return The Semantic Model name.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * Returns the unique ID of the Semantic Model entity.
   *
   * @return The unique ID.
   */
  @Override
  public Long id() {
    return id;
  }

  /**
   * Returns the namespace of the Semantic Model entity.
   *
   * @return The namespace.
   */
  @Override
  public Namespace namespace() {
    return namespace;
  }

  /**
   * Returns the Semantic Model comment.
   *
   * @return The comment, or {@code null} if it is not set.
   */
  @Nullable
  @Override
  public String comment() {
    return comment;
  }

  /**
   * Returns the immutable Semantic Model definition.
   *
   * @return The Semantic Model definition.
   */
  @Override
  public SemanticModelDefinition definition() {
    return definition;
  }

  /**
   * Returns the immutable Gravitino-specific properties of the Semantic Model.
   *
   * @return The properties, or an empty map if none are set.
   */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * Returns the audit information of the Semantic Model entity.
   *
   * @return The audit information.
   */
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /**
   * Returns the Semantic Model entity type.
   *
   * @return {@link EntityType#SEMANTIC_MODEL}.
   */
  @Override
  public EntityType type() {
    return EntityType.SEMANTIC_MODEL;
  }

  /**
   * Validates all declared entity fields.
   *
   * @throws IllegalArgumentException If a required field is missing or has the wrong type.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Entity.super.validate();
  }

  /**
   * Compares this entity with another object for value equality.
   *
   * @param other The object to compare with.
   * @return {@code true} if the objects are equal, otherwise {@code false}.
   */
  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof SemanticModelEntity)) {
      return false;
    }
    SemanticModelEntity that = (SemanticModelEntity) other;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(comment, that.comment)
        && Objects.equals(definition, that.definition)
        && Objects.equals(properties, that.properties)
        && Objects.equals(auditInfo, that.auditInfo);
  }

  /**
   * Returns the value-based hash code of this entity.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    return Objects.hash(id, name, namespace, comment, definition, properties, auditInfo);
  }

  /**
   * Creates a builder for a Semantic Model entity.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /** A builder for {@link SemanticModelEntity}. */
  public static class Builder {

    private final SemanticModelEntity semanticModel;

    private Builder() {
      semanticModel = new SemanticModelEntity();
    }

    /**
     * Sets the unique ID of the Semantic Model entity.
     *
     * @param id The unique ID.
     * @return This builder.
     */
    public Builder withId(Long id) {
      semanticModel.id = id;
      return this;
    }

    /**
     * Sets the name of the Semantic Model entity.
     *
     * @param name The Semantic Model name.
     * @return This builder.
     */
    public Builder withName(String name) {
      semanticModel.name = name;
      return this;
    }

    /**
     * Sets the namespace of the Semantic Model entity.
     *
     * @param namespace The namespace.
     * @return This builder.
     */
    public Builder withNamespace(Namespace namespace) {
      semanticModel.namespace = namespace;
      return this;
    }

    /**
     * Sets the optional Semantic Model comment.
     *
     * @param comment The comment, or {@code null} to leave it unset.
     * @return This builder.
     */
    public Builder withComment(@Nullable String comment) {
      semanticModel.comment = comment;
      return this;
    }

    /**
     * Sets the immutable Semantic Model definition.
     *
     * @param definition The Semantic Model definition.
     * @return This builder.
     */
    public Builder withDefinition(SemanticModelDefinition definition) {
      semanticModel.definition = definition;
      return this;
    }

    /**
     * Sets the Gravitino-specific properties.
     *
     * @param properties The properties, or {@code null} for no properties.
     * @return This builder.
     */
    public Builder withProperties(@Nullable Map<String, String> properties) {
      semanticModel.properties =
          properties == null ? Collections.emptyMap() : ImmutableMap.copyOf(properties);
      return this;
    }

    /**
     * Sets the audit information of the Semantic Model entity.
     *
     * @param auditInfo The audit information.
     * @return This builder.
     */
    public Builder withAuditInfo(AuditInfo auditInfo) {
      semanticModel.auditInfo = auditInfo;
      return this;
    }

    /**
     * Builds and validates the Semantic Model entity.
     *
     * @return The built Semantic Model entity.
     * @throws IllegalArgumentException If a required field is missing or has the wrong type.
     */
    public SemanticModelEntity build() {
      semanticModel.validate();
      return semanticModel;
    }
  }
}
