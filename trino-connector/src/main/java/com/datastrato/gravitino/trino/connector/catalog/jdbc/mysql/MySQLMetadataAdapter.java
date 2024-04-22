/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.jdbc.mysql;

import com.datastrato.gravitino.trino.connector.catalog.jdbc.DefaultJDBCMetadataAdapter;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;

/** Transforming gravitino MySQL metadata to trino. */
public class MySQLMetadataAdapter extends DefaultJDBCMetadataAdapter {

  public MySQLMetadataAdapter(
      List<PropertyMetadata<?>> schemaProperties,
      List<PropertyMetadata<?>> tableProperties,
      List<PropertyMetadata<?>> columnProperties) {

    super(
        schemaProperties,
        tableProperties,
        columnProperties,
        new MySQLDataTypeTransformer(),
        new MySQLTablePropertyConverter());
  }
}
