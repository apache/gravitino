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

package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is generated when an attempt to create a catalog fails due to an
 * exception.
 */
@DeveloperApi
public final class AlterCatalogFailureEvent extends CatalogFailureEvent {
  private final CatalogChange[] catalogChanges;

  /**
   * Constructs an {@code AlterCatalogFailureEvent} instance, capturing detailed information about
   * the failed catalog alteration attempt.
   *
   * @param user The user who initiated the catalog alteration operation.
   * @param identifier The identifier of the catalog that was attempted to be altered.
   * @param exception The exception that was thrown during the catalog alteration operation.
   * @param catalogChanges The changes that were attempted on the catalog.
   */
  public AlterCatalogFailureEvent(
      String user, NameIdentifier identifier, Exception exception, CatalogChange[] catalogChanges) {
    super(user, identifier, exception);
    this.catalogChanges = catalogChanges.clone();
  }

  /**
   * Retrieves the specific changes that were made to the catalog during the alteration process.
   *
   * @return An array of {@link CatalogChange} objects detailing each modification applied to the
   *     catalog.
   */
  public CatalogChange[] catalogChanges() {
    return catalogChanges;
  }
}
