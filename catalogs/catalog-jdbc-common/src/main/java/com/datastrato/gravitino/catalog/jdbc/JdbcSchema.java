/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc;

import com.datastrato.gravitino.connector.BaseSchema;
import lombok.ToString;

/** Represents a Jdbc Schema (Database) entity in the Jdbc schema. */
@ToString
public class JdbcSchema extends BaseSchema {

  private JdbcSchema() {}

  public static class Builder extends BaseSchemaBuilder<Builder, JdbcSchema> {
    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    @Override
    protected JdbcSchema internalBuild() {
      JdbcSchema jdbcSchema = new JdbcSchema();
      jdbcSchema.name = name;
      jdbcSchema.comment = comment;
      jdbcSchema.properties = properties;
      jdbcSchema.auditInfo = auditInfo;
      return jdbcSchema;
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
