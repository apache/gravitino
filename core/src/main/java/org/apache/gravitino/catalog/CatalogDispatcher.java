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

import java.util.function.Consumer;
import org.apache.gravitino.NameIdentifier;

/**
 * {@code CatalogDispatcher} interface acts as a specialization of the {@link SupportsCatalogs}
 * interface. This interface is designed to potentially add custom behaviors or operations related
 * to dispatching or handling catalog-related events or actions that are not covered by the standard
 * {@code SupportsCatalogs} operations.
 */
public interface CatalogDispatcher extends SupportsCatalogs {
  /**
   * Adds a listener that will be notified when a catalog is removed from the cache.
   *
   * <p>Note: Cache eviction is invoked asynchronously but uses a single thread to process removal
   * events. To avoid blocking the eviction thread and delaying subsequent cache operations,
   * listeners should avoid performing heavy operations (such as I/O, network calls, or complex
   * computations) directly. Instead, consider offloading heavy work to a separate thread or
   * executor.
   *
   * @param listener The consumer to be called with the NameIdentifier of the removed catalog.
   */
  void addCatalogCacheRemoveListener(Consumer<NameIdentifier> listener);
}
