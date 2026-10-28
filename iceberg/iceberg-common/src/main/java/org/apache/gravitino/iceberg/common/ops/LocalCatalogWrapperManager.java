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
 * Lakehouse-side {@link CatalogWrapperManager} that caches wrappers with {@link
 * IcebergCatalogWrapperCache} when IRC is not embedded (non-aux).
 *
 * <p>Symmetric to IRC {@code IcebergCatalogWrapperManager}: same cache policy, but creates wrappers
 * via {@link IcebergCatalogWrapper#create(IcebergConfig)} from the caller-supplied config (no IRC
 * config provider).
 */
public final class LocalCatalogWrapperManager implements CatalogWrapperManager, AutoCloseable {

  private final IcebergCatalogWrapperCache cache;

  /**
   * Creates a manager whose cache eviction interval comes from {@code config}.
   *
   * @param config Iceberg configuration (typically the lakehouse catalog props)
   * @return new manager
   */
  public static LocalCatalogWrapperManager create(IcebergConfig config) {
    return new LocalCatalogWrapperManager(IcebergCatalogWrapperCache.create(config));
  }

  private LocalCatalogWrapperManager(IcebergCatalogWrapperCache cache) {
    this.cache = cache;
  }

  /**
   * Unsupported: local managers have no IRC config provider. Use {@link #getCatalogWrapper(String,
   * IcebergConfig)}.
   *
   * @param catalogName catalog name
   * @return never
   */
  @Override
  public IcebergCatalogWrapper getCatalogWrapper(String catalogName) {
    throw new UnsupportedOperationException(
        "LocalCatalogWrapperManager requires an IcebergConfig; use "
            + "getCatalogWrapper(catalogName, icebergConfig)");
  }

  @Override
  public IcebergCatalogWrapper getCatalogWrapper(String catalogName, IcebergConfig icebergConfig) {
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
