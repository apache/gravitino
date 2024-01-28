/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.jdbc.mysql;

import static io.trino.spi.session.PropertyMetadata.stringProperty;

import com.datastrato.gravitino.trino.connector.catalog.HasPropertyMeta;
import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;

public class MySQLPropertyMeta implements HasPropertyMeta {

  static final String TABLE_ENGINE = "engine";

  private static final List<PropertyMetadata<?>> TABLE_PROPERTY_META =
      ImmutableList.of(
          stringProperty(TABLE_ENGINE, "The engine that MySQL table uses", "InnoDB", false));

  @Override
  public List<PropertyMetadata<?>> getTablePropertyMetadata() {
    return TABLE_PROPERTY_META;
  }
}
