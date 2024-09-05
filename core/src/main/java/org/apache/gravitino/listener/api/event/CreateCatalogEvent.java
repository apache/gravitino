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

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.info.CatalogInfo;

/** Represents an event that is activated upon the successful creation of a catalog. */
@DeveloperApi
public class CreateCatalogEvent extends CatalogEvent {
  private final CatalogInfo createdCatalogInfo;

  /**
   * Constructs an instance of {@code CreateCatalogEvent}, capturing essential details about the
   * successful creation of a catalog.
   *
   * @param user The username of the individual who initiated the catalog creation.
   * @param identifier The unique identifier of the catalog that was created.
   * @param createdCatalogInfo The final state of the catalog post-creation.
   */
  public CreateCatalogEvent(
      String user, NameIdentifier identifier, CatalogInfo createdCatalogInfo) {
    super(user, identifier);
    this.createdCatalogInfo = createdCatalogInfo;
  }

  /**
   * Provides the final state of the catalog as it is presented to the user following the successful
   * creation.
   *
   * @return A {@link CatalogInfo} object that encapsulates the detailed characteristics of the
   *     newly created catalog.
   */
  public CatalogInfo createdCatalogInfo() {
    return createdCatalogInfo;
  }
}
