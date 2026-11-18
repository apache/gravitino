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
package org.apache.gravitino.trino.connector.catalog;

import org.apache.gravitino.trino.connector.GravitinoConfig;

/**
 * Service provider interface that lets a jar in the plugin directory contribute a {@link
 * CatalogConnectorAdapter} for a Gravitino catalog provider. Implementations are discovered through
 * {@link java.util.ServiceLoader} when the connector starts.
 */
public interface CatalogConnectorAdapterProvider {

  /**
   * The Gravitino catalog provider this adapter serves, for example {@code jdbc-oracle}.
   *
   * @return the provider name
   */
  String provider();

  /**
   * Creates the adapter for the provider.
   *
   * @param config the Gravitino connector configuration
   * @return a new adapter
   */
  CatalogConnectorAdapter createAdapter(GravitinoConfig config);
}
