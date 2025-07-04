/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.shim;

import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.iceberg.rest.responses.ConfigResponse;

/**
 * Provides REST configuration for Iceberg REST config endpoints. Different Iceberg version may have
 * different implementation.
 */
public interface IcebergRESTConfigProvider {

  /**
   * Initializes the provider with catalog manager.
   *
   * @param catalogManager manager instance for Iceberg catalog backend.
   */
  void initialize(IcebergCatalogWrapperManager catalogManager);

  /**
   * Retrieves configuration settings for a specific warehouse.
   *
   * @param warehouse target warehouse identifier
   * @return configuration response containing Iceberg settings for the given warehouse.
   */
  ConfigResponse getConfig(String warehouse);
}
