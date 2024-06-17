/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.info;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.Schema;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;

/** Provides read-only access to schema information for event listeners. */
@DeveloperApi
public final class SchemaInfo {
  private final String name;
  @Nullable private final String comment;
  private final Map<String, String> properties;
  @Nullable private final Audit audit;

  /**
   * Constructs schema information based on a given schema.
   *
   * @param schema The schema to extract information from.
   */
  public SchemaInfo(Schema schema) {
    this(schema.name(), schema.comment(), schema.properties(), schema.auditInfo());
  }

  /**
   * Constructs schema information with detailed parameters.
   *
   * @param name The name of the schema.
   * @param comment An optional description of the schema.
   * @param properties A map of schema properties.
   * @param audit Optional audit information.
   */
  public SchemaInfo(String name, String comment, Map<String, String> properties, Audit audit) {
    this.name = name;
    this.comment = comment;
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
    this.audit = audit;
  }

  /**
   * Gets the schema name.
   *
   * @return The schema name.
   */
  public String name() {
    return name;
  }

  /**
   * Gets the optional schema comment.
   *
   * @return The schema comment, or null if not provided.
   */
  @Nullable
  public String comment() {
    return comment;
  }

  /**
   * Gets the schema properties.
   *
   * @return An immutable map of schema properties.
   */
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * Gets the optional audit information.
   *
   * @return The audit information, or null if not provided.
   */
  @Nullable
  public Audit audit() {
    return audit;
  }
}
