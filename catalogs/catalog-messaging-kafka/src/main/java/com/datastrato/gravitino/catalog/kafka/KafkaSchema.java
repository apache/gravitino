/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.kafka;

import com.datastrato.gravitino.connector.BaseSchema;

public class KafkaSchema extends BaseSchema {

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends BaseSchemaBuilder<Builder, KafkaSchema> {

    @Override
    protected KafkaSchema internalBuild() {
      KafkaSchema schema = new KafkaSchema();
      schema.name = name;
      schema.comment = comment;
      schema.properties = properties;
      schema.auditInfo = auditInfo;
      return schema;
    }
  }
}
