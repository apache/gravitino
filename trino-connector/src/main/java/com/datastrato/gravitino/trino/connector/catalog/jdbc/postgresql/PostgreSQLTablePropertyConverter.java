/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.jdbc.postgresql;

import com.datastrato.gravitino.catalog.property.PropertyConverter;
import com.datastrato.gravitino.connector.PropertyEntry;
import java.util.Collections;
import java.util.Map;

public class PostgreSQLTablePropertyConverter extends PropertyConverter {

  @Override
  public Map<String, String> engineToGravitinoMapping() {
    // todo: add more properties
    return Collections.emptyMap();
  }

  @Override
  public Map<String, PropertyEntry<?>> gravitinoPropertyMeta() {
    return Collections.emptyMap();
  }
}
