/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.meta;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.Field;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.Metalake;
import com.datastrato.gravitino.StringIdentifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

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
   * @return The audit information as an {@link Audit} instance.
   */
  @Override
  public Audit auditInfo() {
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
    return StringIdentifier.newPropertiesWithoutId(properties);
  }

  /** Builder class for creating instances of {@link BaseMetalake}. */
  public static class Builder {
    private final BaseMetalake metalake;

    /** Constructs a new {@link Builder}. */
    public Builder() {
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
}
