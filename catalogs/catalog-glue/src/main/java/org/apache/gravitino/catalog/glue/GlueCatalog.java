/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.glue;

import java.util.Map;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.capability.Capability;

/**
 * Implementation of an AWS Glue Data Catalog connector in Apache Gravitino.
 *
 * <p>Exposes all Glue table types (Hive, Iceberg, Delta, Parquet) through a single catalog using
 * {@code provider=glue}.
 */
public class GlueCatalog extends BaseCatalog<GlueCatalog> {

  // TODO PR-02: replace stubs with real implementations
  static final GlueCatalogPropertiesMetadata CATALOG_PROPERTIES_METADATA =
      new GlueCatalogPropertiesMetadata();

  static final GlueSchemaPropertiesMetadata SCHEMA_PROPERTIES_METADATA =
      new GlueSchemaPropertiesMetadata();

  static final GlueTablePropertiesMetadata TABLE_PROPERTIES_METADATA =
      new GlueTablePropertiesMetadata();

  /**
   * Returns the short name of the Glue catalog.
   *
   * @return The short name of the catalog.
   */
  @Override
  public String shortName() {
    return "glue";
  }

  /**
   * Creates a new instance of {@link GlueCatalogOperations} with the provided configuration.
   *
   * @param config The configuration map for the Glue catalog operations.
   * @return A new instance of {@link GlueCatalogOperations}.
   */
  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    // TODO PR-04: initialize GlueClient and wire up real operations
    return new GlueCatalogOperations();
  }

  @Override
  public Capability newCapability() {
    // TODO PR-02: implement GlueCatalogCapability
    return new GlueCatalogCapability();
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return CATALOG_PROPERTIES_METADATA;
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    return SCHEMA_PROPERTIES_METADATA;
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    return TABLE_PROPERTIES_METADATA;
  }
}
