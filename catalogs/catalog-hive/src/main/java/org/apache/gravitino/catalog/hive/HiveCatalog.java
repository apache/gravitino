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
package org.apache.gravitino.catalog.hive;

import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.ProxyPlugin;
import org.apache.gravitino.connector.capability.Capability;

/** Implementation of an Apache Hive catalog in Apache Gravitino. */
public class HiveCatalog extends BaseCatalog<HiveCatalog> {

  static final HiveCatalogPropertiesMeta CATALOG_PROPERTIES_METADATA =
      new HiveCatalogPropertiesMeta();

  static final HiveSchemaPropertiesMetadata SCHEMA_PROPERTIES_METADATA =
      new HiveSchemaPropertiesMetadata();

  static final HiveTablePropertiesMetadata TABLE_PROPERTIES_METADATA =
      new HiveTablePropertiesMetadata();

  /**
   * Returns the short name of the Hive catalog.
   *
   * @return The short name of the catalog.
   */
  @Override
  public String shortName() {
    return "hive";
  }

  /**
   * Creates a new instance of {@link HiveCatalogOperations} with the provided configuration.
   *
   * @param config The configuration map for the Hive catalog operations.
   * @return A new instance of {@link HiveCatalogOperations}.
   */
  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    HiveCatalogOperations ops = new HiveCatalogOperations();
    return ops;
  }

  @Override
  public Capability newCapability() {
    return new HiveCatalogCapability();
  }

  @Override
  protected Optional<ProxyPlugin> newProxyPlugin(Map<String, String> config) {
    boolean impersonationEnabled =
        (boolean)
            new HiveCatalogPropertiesMeta()
                .getOrDefault(config, HiveCatalogPropertiesMeta.IMPERSONATION_ENABLE);
    if (!impersonationEnabled) {
      return Optional.empty();
    }
    return Optional.of(new HiveProxyPlugin());
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
