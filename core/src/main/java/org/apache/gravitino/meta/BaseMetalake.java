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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.metalake.MetalakePropertiesMetadata;

/** Base implementation of a Metalake entity. */
@EqualsAndHashCode
@ToString
public class BaseMetalake implements Metalake, Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, "The metalake's unique identifier");
  public static final Field NAME = Field.required("name", String.class, "The metalake's name");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The metalake's comment or description");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties associated with the metalake");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the metalake");

  /** The required field for the schema version of the metalake. */
  public static final Field SCHEMA_VERSION =
      Field.required("version", SchemaVersion.class, "The version of the schema for the metalake");

  public static final PropertiesMetadata PROPERTIES_METADATA = new MetalakePropertiesMetadata();

  private Long id;

  private String name;

  @Nullable private String comment;

  @Nullable private Map<String, String> properties;

  private AuditInfo auditInfo;

  @Getter SchemaVersion version;

  private BaseMetalake() {}

  /**
   * A map of fields and their corresponding values.
   *
   * @return An unmodifiable map containing the entity's fields and values.
   */
  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = new HashMap<>();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(PROPERTIES, properties);
    fields.put(AUDIT_INFO, auditInfo);
    fields.put(SCHEMA_VERSION, version);

    return Collections.unmodifiableMap(fields);
  }

  /**
   * The audit information of the metalake.
   *
   * @return The audit information as an {@link AuditInfo} instance.
   */
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /**
   * The name of the metalake.
   *
   * @return The name of the metalake.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * The unique id of the metalake.
   *
   * @return The unique id of the metalake.
   */
  @Override
  public Long id() {
    return id;
  }

  /**
   * The comment of the metalake.
   *
   * @return The comment of the metalake, or null if not set.
   */
  @Override
  public String comment() {
    return comment;
  }

  /**
   * Retrieves the type of the entity.
   *
   * @return the {@link EntityType#METALAKE} value.
   */
  @Override
  public EntityType type() {
    return EntityType.METALAKE;
  }

  /**
   * Retrieves the properties of the metalake.
   *
   * @return the properties as a map of key-value pairs.
   */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  public PropertiesMetadata propertiesMetadata() {
    return PROPERTIES_METADATA;
  }

  /** Builder class for creating instances of {@link BaseMetalake}. */
  public static class Builder {
    private final BaseMetalake metalake;

    /** Constructs a new {@link Builder}. */
    private Builder() {
      metalake = new BaseMetalake();
    }

    /**
     * Sets the unique identifier of the metalake.
     *
     * @param id the unique identifier of the metalake.
     * @return the builder instance.
     */
    public Builder withId(Long id) {
      metalake.id = id;
      return this;
    }

    /**
     * Sets the name of the metalake.
     *
     * @param name the name of the metalake.
     * @return the builder instance.
     */
    public Builder withName(String name) {
      metalake.name = name;
      return this;
    }

    /**
     * Sets the comment of the metalake.
     *
     * @param comment the comment of the metalake.
     * @return the builder instance.
     */
    public Builder withComment(String comment) {
      metalake.comment = comment;
      return this;
    }

    /**
     * Sets the properties of the metalake.
     *
     * @param properties the properties as a map of key-value pairs.
     * @return the builder instance.
     */
    public Builder withProperties(Map<String, String> properties) {
      metalake.properties = properties;
      return this;
    }

    /**
     * Sets the audit information of the metalake.
     *
     * @param auditInfo the audit information as an {@link AuditInfo} instance.
     * @return the builder instance.
     */
    public Builder withAuditInfo(AuditInfo auditInfo) {
      metalake.auditInfo = auditInfo;
      return this;
    }

    /**
     * Sets the schema version of the metalake.
     *
     * @param version the schema version as a {@link SchemaVersion} instance.
     * @return the builder instance.
     */
    public Builder withVersion(SchemaVersion version) {
      metalake.version = version;
      return this;
    }

    /**
     * Builds the {@link BaseMetalake} instance after validation.
     *
     * @return the constructed and validated {@link BaseMetalake} instance.
     */
    public BaseMetalake build() {
      metalake.validate();
      return metalake;
    }
  }

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}
