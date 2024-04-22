/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.jdbc.postgresql;

import com.datastrato.gravitino.trino.connector.catalog.jdbc.DefaultJDBCMetadataAdapter;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;

/** Transforming gravitino PostgreSQL metadata to trino. */
public class PostgreSQLMetadataAdapter extends DefaultJDBCMetadataAdapter {

  public PostgreSQLMetadataAdapter(
      List<PropertyMetadata<?>> schemaProperties,
      List<PropertyMetadata<?>> tableProperties,
      List<PropertyMetadata<?>> columnProperties) {

    super(
        schemaProperties,
        tableProperties,
        columnProperties,
        new PostgreSQLDataTypeTransformer(),
        new PostgreSQLTablePropertyConverter());
  }
}
