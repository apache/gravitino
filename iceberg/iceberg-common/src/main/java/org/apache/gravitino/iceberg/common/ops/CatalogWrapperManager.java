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
 * <p>{@link StandaloneCatalogWrapperManager} backs metalake lakehouse with {@link
 * IcebergCatalogWrapperCache} (access eviction and GCS token refresh) and creates wrappers via
 * {@link IcebergCatalogWrapper#create(IcebergConfig)}.
 */
public interface CatalogWrapperManager {

  /**
   * Returns the catalog wrapper for {@code catalogName} when the implementation can resolve config
   * without a caller-supplied {@link IcebergConfig}.
   *
   * @param catalogName catalog name
   * @return catalog wrapper
   */
  IcebergCatalogWrapper getCatalogWrapper(String catalogName);

  /**
   * Returns a cached wrapper for {@code catalogName}, or creates one from the caller-supplied
   * {@code icebergConfig}.
   *
   * @param catalogName catalog name
   * @param icebergConfig catalog configuration used on cache miss
   * @return catalog wrapper
   */
  IcebergCatalogWrapper getOrCreateCatalogWrapper(String catalogName, IcebergConfig icebergConfig);

  /**
   * Invalidates cached wrappers for {@code catalogName}.
   *
   * @param catalogName catalog name
   */
  void invalidateCatalogWrapper(String catalogName);
}
