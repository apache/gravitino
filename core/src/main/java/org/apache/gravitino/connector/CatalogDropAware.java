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
package org.apache.gravitino.connector;

import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.Evolving;

/** Supports cleanup that must run only when a catalog is permanently dropped. */
@Evolving
public interface CatalogDropAware {

  /** Performs cleanup after the catalog metadata has been permanently dropped. */
  void onCatalogDropped();

  /**
   * Performs cleanup after the catalog metadata has been permanently dropped, when the catalog
   * registration is unmanaged and its external objects (schemas and the tables inside them) are
   * left in place. Implementations should remove the Gravitino identifier that was written into
   * those external objects, for example the {@code gravitino.identifier} property or the "From
   * Gravitino, DO NOT EDIT" snippet in comments, so that a later catalog that manages the same
   * external objects does not inherit stale identifiers.
   *
   * <p>Cleanup is best-effort: a failure to clear an identifier must not fail the drop. The default
   * implementation only forwards to {@link #onCatalogDropped()}; unmanaged catalogs that never
   * write identifiers to external objects do not need to override this method.
   *
   * @param remainingExternalSchemas the identifiers of the schemas that still exist in the external
   *     system after the catalog registration was dropped. The list is empty for managed catalogs,
   *     whose external objects are dropped together with the registration.
   */
  default void onCatalogDropped(List<NameIdentifier> remainingExternalSchemas) {
    onCatalogDropped();
  }
}
