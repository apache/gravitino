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
package org.apache.gravitino.catalog.lakehouse.hudi;

import java.util.Map;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.PropertiesMetadata;

/** Implementation of an Apache Hudi catalog in Apache Gravitino. */
public class HudiCatalog extends BaseCatalog<HudiCatalog> {

  static final HudiCatalogPropertiesMetadata CATALOG_PROPERTIES_METADATA =
      new HudiCatalogPropertiesMetadata();

  static final HudiSchemaPropertiesMetadata SCHEMA_PROPERTIES_METADATA =
      new HudiSchemaPropertiesMetadata();

  static final HudiTablePropertiesMetadata TABLE_PROPERTIES_METADATA =
      new HudiTablePropertiesMetadata();

  /**
   * @return The short name of the catalog.
   */
  @Override
  public String shortName() {
    return "lakehouse-hudi";
  }

  /**
   * Creates a new instance of {@link HudiCatalogOperations} with the provided configuration.
   *
   * @param config The configuration parameters for creating CatalogOperations.
   * @return A new instance of {@link HudiCatalogOperations}.
   */
  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    return new HudiCatalogOperations();
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
