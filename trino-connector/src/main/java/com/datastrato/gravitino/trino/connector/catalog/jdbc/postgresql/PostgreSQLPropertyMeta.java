/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.jdbc.postgresql;

import static com.datastrato.gravitino.trino.connector.catalog.jdbc.JDBCCommonPropertyNames.AUTO_INCREMENT;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;

import com.datastrato.gravitino.trino.connector.catalog.HasPropertyMeta;
import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;
import java.util.Collections;
import java.util.List;

public class PostgreSQLPropertyMeta implements HasPropertyMeta {

  private static final List<PropertyMetadata<?>> COLUMN_PROPERTY_META =
      ImmutableList.of(booleanProperty(AUTO_INCREMENT, "The auto increment column", false, false));

  @Override
  public List<PropertyMetadata<?>> getTablePropertyMetadata() {
    return Collections.emptyList();
  }

  @Override
  public List<PropertyMetadata<?>> getColumnPropertyMetadata() {
    return COLUMN_PROPERTY_META;
  }
}
