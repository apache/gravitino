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

import java.util.Set;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;

/**
 * This interface is used to create a CatalogConnectorContext builder connector by Gravitino catalog
 */
public interface CatalogConnectorFactory {
  /**
   * Create a CatalogConnectorContext builder by Gravitino catalog
   *
   * @param catalog Gravitino catalog
   * @return CatalogConnectorContext builder
   */
  CatalogConnectorContext.Builder createCatalogConnectorContextBuilder(GravitinoCatalog catalog);

  /**
   * Get supported catalog providers
   *
   * @return catalog providers
   */
  Set<String> getSupportedCatalogProviders();
}
