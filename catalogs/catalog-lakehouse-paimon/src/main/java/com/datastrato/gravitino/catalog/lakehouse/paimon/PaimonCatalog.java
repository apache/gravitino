/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.connector.BaseCatalog;
import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.TableCatalog;
import java.util.Map;

/** Implementation of {@link Catalog} that represents a Paimon catalog in Gravitino. */
public class PaimonCatalog extends BaseCatalog<PaimonCatalog> {

  /** @return The short name of the catalog. */
  @Override
  public String shortName() {
    return "lakehouse-paimon";
  }

  /**
   * Creates a new instance of {@link PaimonCatalogOperations} with the provided configuration.
   *
   * @param config The configuration map for the Paimon catalog operations.
   * @return A new instance of {@link PaimonCatalogOperations}.
   */
  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    return new PaimonCatalogOperations();
  }

  /** @return The Paimon catalog operations as {@link PaimonCatalogOperations}. */
  @Override
  public SupportsSchemas asSchemas() {
    return (PaimonCatalogOperations) ops();
  }

  /** @return The Paimon catalog operations as {@link PaimonCatalogOperations}. */
  @Override
  public TableCatalog asTableCatalog() {
    return (PaimonCatalogOperations) ops();
  }
}
