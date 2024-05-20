/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.kafka;

import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import java.util.Collections;
import java.util.Map;

public class KafkaSchemaPropertiesMetadata extends BasePropertiesMetadata {

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return Collections.emptyMap();
  }
}
