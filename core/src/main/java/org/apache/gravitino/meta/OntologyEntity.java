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

import com.google.common.base.Preconditions;
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
import org.apache.gravitino.ontology.Ontology;
import org.apache.gravitino.ontology.OntologyDefinition;

/**
 * A metadata-store entity representing a metalake-level Apache Ossie ontology.
 *
 * <p>An ontology is addressed by name alone inside its metalake, so its namespace has exactly one
 * level, the metalake name, like the namespace of a catalog. The store keeps no catalog or schema
 * identifier for it, and {@link #namespace()} is carried only so that the generic entity machinery
 * can build a name identifier for the row.
 *
 * <p>The status lives on this identity exactly like the comment and the definition: the entity is
 * one resolved snapshot of the ontology at its current version.
 */
@ToString
public class OntologyEntity implements Ontology, Entity, Auditable, HasIdentifier {

  /** The unique ID field of the ontology entity. */
  public static final Field ID =
      Field.required("id", Long.class, "The unique id of the ontology entity.");

  /** The name field of the ontology entity. */
  public static final Field NAME =
      Field.required("name", String.class, "The name of the ontology entity.");

  /** The optional comment field of the ontology entity. */
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment or description of the ontology entity.");

  /** The status field of the ontology entity. */
  public static final Field STATUS =
      Field.required("status", Ontology.Status.class, "The status of the ontology entity.");

  /** The immutable definition field of the ontology entity. */
  public static final Field DEFINITION =
      Field.required(
          "definition",
          OntologyDefinition.class,
          "The immutable concept graph of the ontology entity.");

  /** The properties field of the ontology entity. */
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The Gravitino-specific properties of the entity.");

  /** The audit information field of the ontology entity. */
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the ontology entity.");

  private Long id;
  private String name;
  private Namespace namespace;
  private Ontology.Status status;
  private OntologyDefinition definition;
  private Map<String, String> properties = Collections.emptyMap();
  private AuditInfo auditInfo;

  @Nullable private String comment;

  private OntologyEntity() {}

  /**
   * Returns the fields and values of this ontology entity.
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
    fields.put(COMMENT, comment);
    fields.put(STATUS, status);
    fields.put(DEFINITION, definition);
    fields.put(PROPERTIES, properties);
    fields.put(AUDIT_INFO, auditInfo);
    return Collections.unmodifiableMap(fields);
  }

  /**
   * Returns the ontology name.
   *
   * @return The ontology name.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * Returns the unique ID of the ontology entity.
   *
   * @return The unique ID.
   */
  @Override
  public Long id() {
    return id;
  }

  /**
   * Returns the namespace of the ontology entity, which holds the metalake name.
   *
   * @return The namespace.
   */
  @Override
  public Namespace namespace() {
    return namespace;
  }

  /**
   * Returns the ontology comment.
   *
   * @return The comment, or {@code null} if it is not set.
   */
  @Nullable
  @Override
  public String comment() {
    return comment;
  }

  /**
   * Returns the immutable Gravitino-specific properties of the ontology.
   *
   * @return The properties, or an empty map if none are set.
   */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * Returns the ontology status.
   *
   * @return The status.
   */
  @Override
  public Ontology.Status status() {
    return status;
  }

  /**
   * Returns the immutable concept graph of the ontology.
   *
   * @return The ontology definition.
   */
  @Override
  public OntologyDefinition definition() {
    return definition;
  }

  /**
   * Returns the audit information of the ontology entity.
   *
   * @return The audit information.
   */
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /**
   * Returns the ontology entity type.
   *
   * @return {@link EntityType#ONTOLOGY}.
   */
  @Override
  public EntityType type() {
    return EntityType.ONTOLOGY;
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
    if (!(other instanceof OntologyEntity)) {
      return false;
    }
    OntologyEntity that = (OntologyEntity) other;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(comment, that.comment)
        && status == that.status
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
    return Objects.hash(id, name, namespace, comment, status, definition, properties, auditInfo);
  }

  /**
   * Creates a builder for an ontology entity.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /** A builder for {@link OntologyEntity}. */
  public static class Builder {

    private final OntologyEntity ontology;

    private Builder() {
      ontology = new OntologyEntity();
    }

    /**
     * Sets the unique ID of the ontology entity.
     *
     * @param id The unique ID.
     * @return This builder.
     */
    public Builder withId(Long id) {
      ontology.id = id;
      return this;
    }

    /**
     * Sets the name of the ontology entity.
     *
     * @param name The ontology name.
     * @return This builder.
     */
    public Builder withName(String name) {
      ontology.name = name;
      return this;
    }

    /**
     * Sets the namespace of the ontology entity, which holds the metalake name.
     *
     * @param namespace The one-level metalake namespace.
     * @return This builder.
     */
    public Builder withNamespace(Namespace namespace) {
      ontology.namespace = namespace;
      return this;
    }

    /**
     * Sets the optional ontology comment.
     *
     * @param comment The comment, or {@code null} to leave it unset.
     * @return This builder.
     */
    public Builder withComment(@Nullable String comment) {
      ontology.comment = comment;
      return this;
    }

    /**
     * Sets the ontology status.
     *
     * @param status The status.
     * @return This builder.
     */
    public Builder withStatus(Ontology.Status status) {
      ontology.status = status;
      return this;
    }

    /**
     * Sets the immutable ontology definition.
     *
     * @param definition The concept graph of the ontology.
     * @return This builder.
     */
    public Builder withDefinition(OntologyDefinition definition) {
      ontology.definition = definition;
      return this;
    }

    /**
     * Sets the Gravitino-specific properties.
     *
     * @param properties The properties, or {@code null} for no properties.
     * @return This builder.
     */
    public Builder withProperties(@Nullable Map<String, String> properties) {
      ontology.properties =
          properties == null ? Collections.emptyMap() : ImmutableMap.copyOf(properties);
      return this;
    }

    /**
     * Sets the audit information of the ontology entity.
     *
     * @param auditInfo The audit information.
     * @return This builder.
     */
    public Builder withAuditInfo(AuditInfo auditInfo) {
      ontology.auditInfo = auditInfo;
      return this;
    }

    /**
     * Builds and validates the ontology entity.
     *
     * <p>The namespace is not one of the declared entity fields, so it is checked here rather than
     * by {@link OntologyEntity#validate()}.
     *
     * @return The built ontology entity.
     * @throws IllegalArgumentException If a required field is missing, has the wrong type, or the
     *     namespace is not set.
     */
    public OntologyEntity build() {
      Preconditions.checkArgument(
          ontology.namespace != null, "The namespace of an ontology entity must not be null");
      ontology.validate();
      return ontology;
    }
  }
}
