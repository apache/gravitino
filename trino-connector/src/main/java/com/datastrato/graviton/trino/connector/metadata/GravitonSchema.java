/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.metadata;

import com.datastrato.graviton.rel.Schema;
import java.util.Map;

/** Help Graviton connector access SchemaMetadata from graviton client. */
public class GravitonSchema {

  private final String schemaName;
  private final Map<String, String> properties;
  private final String comment;

  public GravitonSchema(Schema schema) {
    this.schemaName = schema.name();
    this.properties = schema.properties();
    this.comment = schema.comment();
  }

  public GravitonSchema(String schemaName, Map<String, String> properties, String comment) {
    this.schemaName = schemaName;
    this.properties = properties;
    this.comment = comment;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public String getName() {
    return schemaName;
  }

  public String getComment() {
    return comment;
  }
}
