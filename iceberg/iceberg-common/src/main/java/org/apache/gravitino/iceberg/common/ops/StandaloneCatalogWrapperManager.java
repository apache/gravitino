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

import org.apache.gravitino.iceberg.common.IcebergConfig;

/**
 * Standalone (non-auxiliary) {@link CatalogWrapperManager} backed by {@link
 * IcebergCatalogWrapperCache} when IRC is not embedded in the Gravitino server.
 *
 * <p>Same cache policy as IRC {@code IcebergCatalogWrapperManager}, but creates wrappers via {@link
 * IcebergCatalogWrapper#create(IcebergConfig)} from the caller-supplied config.
 */
public final class StandaloneCatalogWrapperManager implements CatalogWrapperManager, AutoCloseable {

  private final IcebergCatalogWrapperCache cache;

  /**
   * @param config Iceberg configuration (eviction interval and lakehouse catalog props)
   */
  public StandaloneCatalogWrapperManager(IcebergConfig config) {
    this.cache = IcebergCatalogWrapperCache.create(config);
  }

  /**
   * Unsupported: no IRC config provider. Use {@link #getOrCreateCatalogWrapper(String,
   * IcebergConfig)}.
   *
   * @param catalogName catalog name
   * @return never
   */
  @Override
  public IcebergCatalogWrapper getCatalogWrapper(String catalogName) {
    throw new UnsupportedOperationException(
        "StandaloneCatalogWrapperManager has no IRC config provider; use "
            + "getOrCreateCatalogWrapper(catalogName, icebergConfig)");
  }

  @Override
  public IcebergCatalogWrapper getOrCreateCatalogWrapper(
      String catalogName, IcebergConfig icebergConfig) {
    return cache.get(catalogName, name -> IcebergCatalogWrapper.create(icebergConfig));
  }

  @Override
  public void invalidateCatalogWrapper(String catalogName) {
    cache.invalidate(catalogName);
  }

  @Override
  public void close() {
    cache.close();
  }
}
