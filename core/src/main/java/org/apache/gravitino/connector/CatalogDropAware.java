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
   * Whether this connector implements external identifier cleanup through {@link
   * #onCatalogDropped(List)}.
   *
   * <p>The default is false so existing resource-only callbacks do not cause external schema
   * listing. A connector must explicitly opt in when it implements identifier cleanup.
   *
   * @return true to receive external schema identifiers, or false to use the no-argument callback
   */
  default boolean supportsExternalIdentifierCleanup() {
    return false;
  }

  /**
   * Performs cleanup after the catalog metadata has been permanently dropped, when the catalog
   * registration is unmanaged and its external objects (schemas and the tables inside them) are
   * left in place. Implementations should remove the Gravitino identifier that was written into
   * those external objects, for example the {@code gravitino.identifier} property or the "From
   * Gravitino, DO NOT EDIT" snippet in comments, so that a later catalog that manages the same
   * external objects does not inherit stale identifiers.
   *
   * <p>This callback is invoked only when {@link #supportsExternalIdentifierCleanup()} returns
   * true. The schema names are discovery hints, not proof that an external identifier belongs to
   * the dropped catalog. Implementations must verify ownership before removing identifiers and
   * preserve user properties and comments. This interface does not provide external atomicity or
   * protect against concurrent changes in the external system.
   *
   * <p>Cleanup is best-effort: the caller logs callback failures and continues the drop. This does
   * not bound the time spent in external I/O. The default implementation only forwards to {@link
   * #onCatalogDropped()} and does not remove identifiers. Overrides must also perform the resource
   * cleanup required by {@link #onCatalogDropped()}, including when discovery or identifier cleanup
   * fails.
   *
   * @param remainingExternalSchemas the identifiers of the schemas that still exist in the external
   *     system after the catalog registration was dropped. The list is empty for managed catalogs,
   *     whose external objects are dropped together with the registration, or when schema listing
   *     fails. An empty list must not be interpreted as authorization to discover or clean
   *     arbitrary external objects.
   */
  default void onCatalogDropped(List<NameIdentifier> remainingExternalSchemas) {
    onCatalogDropped();
  }
}
