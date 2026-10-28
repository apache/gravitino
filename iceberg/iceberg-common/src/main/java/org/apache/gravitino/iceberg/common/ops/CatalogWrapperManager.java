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
 * Resolves {@link IcebergCatalogWrapper} instances by catalog name.
 *
 * <p>Implementations share {@link IcebergCatalogWrapperCache} eviction/token policy:
 *
 * <ul>
 *   <li>IRC {@code IcebergCatalogWrapperManager} — config provider + {@code CatalogWrapperForREST}
 *   <li>{@link StandaloneCatalogWrapperManager} — standalone lakehouse (no embedded IRC), creates
 *       via {@link IcebergCatalogWrapper#create(IcebergConfig)}
 * </ul>
 *
 * <p>Symmetric to {@code CatalogManager} used by {@code
 * DynamicIcebergConfigProvider.InternalCatalogFetcher}.
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
   * Returns a cached wrapper for {@code catalogName}, or creates one from the caller-supplied
   * {@code icebergConfig}. Unlike {@link #getCatalogWrapper(String)}, config is not loaded from the
   * IRC provider (used by metalake lakehouse with static-config-provider).
   *
   * @param catalogName catalog name
   * @param icebergConfig catalog configuration used on cache miss
   * @return catalog wrapper
   */
  IcebergCatalogWrapper getOrCreateCatalogWrapper(String catalogName, IcebergConfig icebergConfig);

  /**
   * Invalidates a cached wrapper for {@code catalogName}.
   *
   * @param catalogName catalog name
   */
  void invalidateCatalogWrapper(String catalogName);

  /**
   * Holds {@code wrapper} open until {@link #releaseCatalogWrapper} when a newer catalog version
   * replaces the cache entry.
   *
   * @param wrapper wrapper this catalog lease will keep using
   */
  default void acquireCatalogWrapper(IcebergCatalogWrapper wrapper) {}

  /**
   * Drops the lease taken by {@link #acquireCatalogWrapper}. The wrapper is closed once the cache
   * and every lease have released it.
   *
   * @param wrapper wrapper previously passed to {@link #acquireCatalogWrapper}
   */
  default void releaseCatalogWrapper(IcebergCatalogWrapper wrapper) {}
}
