/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.metadata;

import com.datastrato.graviton.rel.Schema;
import java.util.Map;

/** Help Graviton connector access SchemaMetadata from graviton client. */
public class GravitonSchema {

  private final Schema schema;

  public GravitonSchema(Schema schema) {
    this.schema = schema;
  }

  public Map<String, String> properties() {
    return schema.properties();
  }

  public String name() {
    return schema.name();
  }

  public String comment() {
    return schema.comment();
  }
}
