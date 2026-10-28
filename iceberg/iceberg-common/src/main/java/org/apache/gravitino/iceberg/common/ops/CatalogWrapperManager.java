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
package org.apache.gravitino.iceberg.common.ops;

/**
 * Resolves {@link IcebergCatalogWrapper} instances by catalog name for the Iceberg REST service.
 *
 * <p>Implemented by IRC {@code IcebergCatalogWrapperManager}. Lakehouse metalake ops keep a
 * separate, owned {@link IcebergCatalogWrapper} (and refresh via {@code CatalogManager}'s catalog
 * cache); they do not share wrapper instances with this manager.
 */
public interface CatalogWrapperManager {

  /**
   * Returns the catalog wrapper for {@code catalogName}, creating and caching it from the IRC
   * config provider when absent.
   *
   * @param catalogName catalog name
   * @return catalog wrapper
   */
  IcebergCatalogWrapper getCatalogWrapper(String catalogName);

  /**
   * Invalidates cached wrappers for {@code catalogName}.
   *
   * @param catalogName catalog name
   */
  void invalidateCatalogWrapper(String catalogName);
}
