/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.hive;

import com.datastrato.gravitino.trino.connector.catalog.PropertyConverter;
import java.util.Map;

public class HiveSchemaPropertyConverter implements PropertyConverter {

  @Override
  public Map<String, String> toTrinoProperties(Map<String, String> properties) {
    return PropertyConverter.super.toTrinoProperties(properties);
  }

  @Override
  public Map<String, Object> toGravitinoProperties(Map<String, Object> properties) {
    return PropertyConverter.super.toGravitinoProperties(properties);
  }
}
