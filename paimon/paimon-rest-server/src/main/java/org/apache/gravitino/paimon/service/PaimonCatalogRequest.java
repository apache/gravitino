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
package org.apache.gravitino.paimon.service;

import javax.inject.Inject;
import javax.inject.Provider;
import org.apache.gravitino.paimon.common.PaimonCatalogWrapper;
import org.apache.gravitino.paimon.service.PaimonCatalogWrapperManager.CatalogLease;
import org.glassfish.jersey.server.CloseableService;

/** Resolves a Paimon catalog and holds it until Jersey finishes the request. */
public class PaimonCatalogRequest {
  private final PaimonCatalogWrapperManager manager;
  private final Provider<CloseableService> closeables;

  /**
   * Creates a resolver for the Jersey request lifecycle.
   *
   * @param manager the backend catalog manager
   * @param closeables the current request's closeable service
   */
  @Inject
  public PaimonCatalogRequest(
      PaimonCatalogWrapperManager manager, Provider<CloseableService> closeables) {
    this.manager = manager;
    this.closeables = closeables;
  }

  /**
   * Acquires a catalog and registers it for cleanup when the request completes.
   *
   * @param name the catalog name
   * @return the catalog lease
   */
  public CatalogLease acquire(String name) {
    CloseableService closeableService = closeables.get();
    CatalogLease lease = manager.acquire(name);
    if (!closeableService.add(lease)) {
      lease.close();
      throw new IllegalStateException("Could not register the Paimon catalog for request cleanup.");
    }
    return lease;
  }

  /**
   * Returns the catalog for the request.
   *
   * @param name the catalog name
   * @return the backend catalog
   */
  public PaimonCatalogWrapper getCatalogWrapper(String name) {
    return acquire(name).wrapper();
  }
}
