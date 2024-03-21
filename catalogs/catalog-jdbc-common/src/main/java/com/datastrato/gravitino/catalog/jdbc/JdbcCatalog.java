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
import com.datastrato.gravitino.connector.PropertyEntry;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.TableCatalog;
import java.util.Collections;
import java.util.Map;

/** Implementation of an Jdbc catalog in Gravitino. */
public abstract class JdbcCatalog extends BaseCatalog<JdbcCatalog> {

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
            createJdbcTablePropertiesMetadata(),
            createJdbcColumnDefaultValueConverter());
    return ops;
  }

  /** @return The Jdbc catalog operations as {@link JdbcCatalogOperations}. */
  @Override
  public SupportsSchemas asSchemas() {
    return (JdbcCatalogOperations) ops();
  }

  /** @return The Jdbc catalog operations as {@link JdbcCatalogOperations}. */
  @Override
  public TableCatalog asTableCatalog() {
    return (JdbcCatalogOperations) ops();
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

  /** @return The {@link JdbcTablePropertiesMetadata} to be used by the catalog to manage table */
  protected JdbcTablePropertiesMetadata createJdbcTablePropertiesMetadata() {
    return new JdbcTablePropertiesMetadata() {

      @Override
      protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
        return Collections.emptyMap();
      }
    };
  }

  /** @return The {@link JdbcColumnDefaultValueConverter} to be used by the catalog */
  protected abstract JdbcColumnDefaultValueConverter createJdbcColumnDefaultValueConverter();
}
