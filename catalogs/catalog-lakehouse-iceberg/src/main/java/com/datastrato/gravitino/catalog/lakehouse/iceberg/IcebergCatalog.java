/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg;

import com.datastrato.gravitino.connector.BaseCatalog;
import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.connector.capability.Capability;
import java.util.Map;

/** Implementation of an Iceberg catalog in Gravitino. */
public class IcebergCatalog extends BaseCatalog<IcebergCatalog> {

  static final IcebergCatalogPropertiesMetadata CATALOG_PROPERTIES_META =
      new IcebergCatalogPropertiesMetadata();

  static final IcebergSchemaPropertiesMetadata SCHEMA_PROPERTIES_META =
      new IcebergSchemaPropertiesMetadata();

  static final IcebergTablePropertiesMetadata TABLE_PROPERTIES_META =
      new IcebergTablePropertiesMetadata();

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

  @Override
  public Capability newCapability() {
    return new IcebergCatalogCapability();
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    return TABLE_PROPERTIES_META;
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
