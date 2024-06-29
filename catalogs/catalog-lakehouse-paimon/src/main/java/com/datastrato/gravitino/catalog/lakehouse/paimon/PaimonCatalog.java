/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.connector.BaseCatalog;
import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.connector.capability.Capability;
import java.util.Map;

/** Implementation of {@link Catalog} that represents a Paimon catalog in Gravitino. */
public class PaimonCatalog extends BaseCatalog<PaimonCatalog> {

  static final PaimonCatalogPropertiesMetadata CATALOG_PROPERTIES_META =
      new PaimonCatalogPropertiesMetadata();

  static final PaimonSchemaPropertiesMetadata SCHEMA_PROPERTIES_META =
      new PaimonSchemaPropertiesMetadata();

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

  @Override
  public Capability newCapability() {
    return new PaimonCatalogCapability();
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "The catalog does not support table properties metadata");
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return CATALOG_PROPERTIES_META;
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    return SCHEMA_PROPERTIES_META;
  }
}
