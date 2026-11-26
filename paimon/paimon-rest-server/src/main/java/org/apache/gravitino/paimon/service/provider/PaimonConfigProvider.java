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
package org.apache.gravitino.paimon.service.provider;

import java.io.Closeable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Provides static backend configuration for a REST-visible catalog name. */
public interface PaimonConfigProvider extends Closeable {

  /**
   * Initializes the provider with the service configuration (the {@code gravitino.paimon-rest.}
   * prefix is stripped by the caller).
   *
   * @param properties the service configuration
   */
  void initialize(Map<String, String> properties);

  /**
   * Returns the backend catalog configuration for a catalog name.
   *
   * @param catalogName the REST-visible catalog name
   * @return the configuration, or empty when the catalog is unknown to this provider
   */
  Optional<CatalogConfig> getCatalogConfig(String catalogName);

  /**
   * Returns the names of all catalogs known to this provider.
   *
   * @return the catalog names
   */
  Set<String> catalogNames();

  /** Backend options for a statically configured catalog. */
  final class CatalogConfig {
    private final Map<String, String> properties;

    /**
     * Creates the resolved catalog configuration.
     *
     * @param properties backend catalog options
     */
    public CatalogConfig(Map<String, String> properties) {
      this.properties = Map.copyOf(properties);
    }

    /**
     * @return the backend catalog options
     */
    public Map<String, String> properties() {
      return properties;
    }
  }
}
