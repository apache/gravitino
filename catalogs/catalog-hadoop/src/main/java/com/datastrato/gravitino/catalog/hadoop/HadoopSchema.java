/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hadoop;

import com.datastrato.gravitino.connector.BaseSchema;

public class HadoopSchema extends BaseSchema {

  public static class Builder extends BaseSchemaBuilder<Builder, HadoopSchema> {

    @Override
    protected HadoopSchema internalBuild() {
      HadoopSchema schema = new HadoopSchema();
      schema.name = name;
      schema.comment = comment;
      schema.properties = properties;
      schema.auditInfo = auditInfo;
      return schema;
    }
  }
}
