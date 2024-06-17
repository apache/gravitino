/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc;

import com.datastrato.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import com.datastrato.gravitino.connector.BaseCatalog;
import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import java.util.Collections;
import java.util.Map;

/** Implementation of an Jdbc catalog in Gravitino. */
public abstract class JdbcCatalog extends BaseCatalog<JdbcCatalog> {

  private static final JdbcCatalogPropertiesMetadata CATALOG_PROPERTIES_META =
      new JdbcCatalogPropertiesMetadata();

  private static final JdbcSchemaPropertiesMetadata SCHEMA_PROPERTIES_META =
      new JdbcSchemaPropertiesMetadata();

  private static final JdbcTablePropertiesMetadata TABLE_PROPERTIES_META =
      new JdbcTablePropertiesMetadata() {
        @Override
        protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
          return Collections.emptyMap();
        }
      };

  /**
   * Creates a new instance of {@link JdbcCatalogOperations} with the provided configuration.
   *
   * @param config The configuration map for the Jdbc catalog operations.
   * @return A new instance of {@link JdbcCatalogOperations}.
   */
  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    JdbcTypeConverter jdbcTypeConverter = createJdbcTypeConverter();
    JdbcCatalogOperations ops =
        new JdbcCatalogOperations(
            createExceptionConverter(),
            jdbcTypeConverter,
            createJdbcDatabaseOperations(),
            createJdbcTableOperations(),
            createJdbcColumnDefaultValueConverter());
    return ops;
  }

  /** @return The {@link JdbcExceptionConverter} to be used by the catalog. */
  protected JdbcExceptionConverter createExceptionConverter() {
    return new JdbcExceptionConverter() {};
  }

  /** @return The {@link JdbcTypeConverter} to be used by the catalog. */
  protected abstract JdbcTypeConverter createJdbcTypeConverter();

  /**
   * @return The {@link JdbcDatabaseOperations} to be used by the catalog to manage databases in the
   */
  protected abstract JdbcDatabaseOperations createJdbcDatabaseOperations();

  /** @return The {@link JdbcTableOperations} to be used by the catalog to manage tables in the */
  protected abstract JdbcTableOperations createJdbcTableOperations();

  /** @return The {@link JdbcColumnDefaultValueConverter} to be used by the catalog */
  protected abstract JdbcColumnDefaultValueConverter createJdbcColumnDefaultValueConverter();

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return CATALOG_PROPERTIES_META;
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    return SCHEMA_PROPERTIES_META;
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    return TABLE_PROPERTIES_META;
  }
}
