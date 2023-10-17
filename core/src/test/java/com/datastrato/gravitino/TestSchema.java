/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.catalog.rel.BaseSchema;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
public class TestSchema extends BaseSchema {

  private TestSchema() {}

  public static class Builder extends BaseSchema.BaseSchemaBuilder<Builder, TestSchema> {

    @Override
    protected TestSchema internalBuild() {
      TestSchema schema = new TestSchema();

      schema.name = name;
      schema.comment = comment;
      schema.properties = properties;
      schema.auditInfo = auditInfo;

      return schema;
    }
  }
}
