/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.metadata;

import com.datastrato.gravitino.rel.Schema;
import java.util.Map;

/** Help Gravitino connector access SchemaMetadata from gravitino client. */
public class GravitinoSchema {

  private final String schemaName;
  private final Map<String, String> properties;
  private final String comment;

  public GravitinoSchema(Schema schema) {
    this.schemaName = schema.name();
    this.properties = schema.properties();
    this.comment = schema.comment();
  }

  public GravitinoSchema(String schemaName, Map<String, String> properties, String comment) {
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
