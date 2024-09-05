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
import org.apache.gravitino.listener.api.info.CatalogInfo;

/** Represents an event triggered upon the successful creation of a catalog. */
@DeveloperApi
public final class AlterCatalogEvent extends CatalogEvent {
  private final CatalogInfo updatedCatalogInfo;
  private final CatalogChange[] catalogChanges;

  /**
   * Constructs an instance of {@code AlterCatalogEvent}, encapsulating the key details about the
   * successful alteration of a catalog.
   *
   * @param user The username of the individual responsible for initiating the catalog alteration.
   * @param identifier The unique identifier of the altered catalog, serving as a clear reference
   *     point for the catalog in question.
   * @param catalogChanges An array of {@link CatalogChange} objects representing the specific
   *     changes applied to the catalog during the alteration process.
   * @param updatedCatalogInfo The post-alteration state of the catalog.
   */
  public AlterCatalogEvent(
      String user,
      NameIdentifier identifier,
      CatalogChange[] catalogChanges,
      CatalogInfo updatedCatalogInfo) {
    super(user, identifier);
    this.catalogChanges = catalogChanges.clone();
    this.updatedCatalogInfo = updatedCatalogInfo;
  }

  /**
   * Retrieves the final state of the catalog as it was returned to the user after successful
   * creation.
   *
   * @return A {@link CatalogInfo} instance encapsulating the comprehensive details of the newly
   *     created catalog.
   */
  public CatalogInfo updatedCatalogInfo() {
    return updatedCatalogInfo;
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
