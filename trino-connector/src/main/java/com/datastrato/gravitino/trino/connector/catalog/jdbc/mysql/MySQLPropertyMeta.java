/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.jdbc.mysql;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

import com.datastrato.gravitino.trino.connector.catalog.HasPropertyMeta;
import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;

public class MySQLPropertyMeta implements HasPropertyMeta {

  static final String TABLE_ENGINE = "engine";
  static final String TABLE_AUTO_INCREMENT_OFFSET = "auto_increment_offset";

  private static final List<PropertyMetadata<?>> TABLE_PROPERTY_META =
      ImmutableList.of(
          stringProperty(TABLE_ENGINE, "The engine that MySQL table uses", "InnoDB", false),
          stringProperty(
              TABLE_AUTO_INCREMENT_OFFSET, "The auto increment offset for the table", null, false));

  public static final String AUTO_INCREMENT = "auto_increment";
  private static final List<PropertyMetadata<?>> COLUMN_PROPERTY_META =
      ImmutableList.of(booleanProperty(AUTO_INCREMENT, "The auto increment column", false, false));

  @Override
  public List<PropertyMetadata<?>> getTablePropertyMetadata() {
    return TABLE_PROPERTY_META;
  }

  @Override
  public List<PropertyMetadata<?>> getColumnPropertyMetadata() {
    return COLUMN_PROPERTY_META;
  }
}
