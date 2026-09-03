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
package org.apache.gravitino.catalog;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager.CatalogWrapper;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.utils.IsolatedClassLoader;

/**
 * A lease on a {@link CatalogWrapper} held for the duration of one catalog operation.
 *
 * <p>While the lease is held, the wrapper's catalog instance and its {@link IsolatedClassLoader}
 * stay alive even if the catalog cache evicts the wrapper concurrently (expiry, explicit
 * invalidation, or remote change-log invalidation). The resources are released once the wrapper is
 * retired and its last lease is closed, so an operation can never observe a half-closed catalog.
 *
 * <p>Leases are obtained from {@link CatalogManager#acquireCatalogLease(NameIdentifier)} and must
 * be closed exactly once, ideally with try-with-resources:
 *
 * <pre>{@code
 * try (CatalogLease lease = catalogManager.acquireCatalogLease(ident)) {
 *   return lease.wrapper().doWithTableOps(ops -> ops.loadTable(tableIdent));
 * }
 * }</pre>
 */
public final class CatalogLease implements AutoCloseable {

  private final CatalogWrapper wrapper;
  private final AtomicBoolean released = new AtomicBoolean(false);

  CatalogLease(CatalogWrapper wrapper) {
    this.wrapper = wrapper;
  }

  /**
   * Returns the leased catalog wrapper.
   *
   * @return the leased catalog wrapper, guaranteed to stay usable until this lease is closed.
   */
  public CatalogWrapper wrapper() {
    return wrapper;
  }

  /**
   * Returns the catalog of the leased wrapper.
   *
   * @return the leased catalog, guaranteed to stay usable until this lease is closed.
   */
  public BaseCatalog catalog() {
    return wrapper.catalog();
  }

  /** Releases the lease. Closing an already closed lease is a no-op. */
  @Override
  public void close() {
    if (released.compareAndSet(false, true)) {
      wrapper.release();
    }
  }
}
