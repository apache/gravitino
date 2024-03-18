/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.jdbc.mysql;

import static com.datastrato.gravitino.trino.connector.catalog.jdbc.mysql.MySQLPropertyMeta.TABLE_AUTO_INCREMENT_OFFSET;
import static com.datastrato.gravitino.trino.connector.catalog.jdbc.mysql.MySQLPropertyMeta.TABLE_ENGINE;

import com.datastrato.gravitino.catalog.mysql.MysqlTablePropertiesMetadata;
import com.datastrato.gravitino.catalog.property.PropertyConverter;
import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.datastrato.gravitino.shaded.com.google.common.collect.ImmutableMap;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import org.apache.commons.collections4.bidimap.TreeBidiMap;

public class MySQLTablePropertyConverter extends PropertyConverter {

  private final BasePropertiesMetadata mysqlTablePropertiesMetadata =
      new MysqlTablePropertiesMetadata();

  // TODO (yuqi) add more properties
  @VisibleForTesting
  static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              .put(TABLE_ENGINE, MysqlTablePropertiesMetadata.GRAVITINO_ENGINE_KEY)
              .put(
                  TABLE_AUTO_INCREMENT_OFFSET,
                  MysqlTablePropertiesMetadata.GRAVITINO_AUTO_INCREMENT_OFFSET_KEY)
              .build());

  @Override
  public Map<String, String> engineToGravitinoMapping() {
    return TRINO_KEY_TO_GRAVITINO_KEY;
  }

  @Override
  public Map<String, PropertyEntry<?>> gravitinoPropertyMeta() {
    return mysqlTablePropertiesMetadata.propertyEntries();
  }
}
