/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg;

import com.datastrato.gravitino.connector.BaseCatalog;
import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.TableCatalog;
import java.util.Map;

/** Implementation of an Iceberg catalog in Gravitino. */
public class IcebergCatalog extends BaseCatalog<IcebergCatalog> {

  /** @return The short name of the catalog. */
  @Override
  public String shortName() {
    return "lakehouse-iceberg";
  }

  /**
   * Creates a new instance of {@link IcebergCatalogOperations} with the provided configuration.
   *
   * @param config The configuration map for the Iceberg catalog operations.
   * @return A new instance of {@link IcebergCatalogOperations}.
   */
  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    IcebergCatalogOperations ops = new IcebergCatalogOperations();
    return ops;
  }

  /** @return The Iceberg catalog operations as {@link IcebergCatalogOperations}. */
  @Override
  public SupportsSchemas asSchemas() {
    return (IcebergCatalogOperations) ops();
  }

  /** @return The Iceberg catalog operations as {@link IcebergCatalogOperations}. */
  @Override
  public TableCatalog asTableCatalog() {
    return (IcebergCatalogOperations) ops();
  }
}
