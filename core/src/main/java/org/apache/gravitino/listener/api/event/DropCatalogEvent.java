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

/** Represents an event that is generated after a catalog is successfully dropped. */
@DeveloperApi
public final class DropCatalogEvent extends CatalogEvent {
  private final boolean isExists;

  /**
   * Constructs a new {@code DropCatalogEvent} instance, encapsulating information about the outcome
   * of a catalog drop operation.
   *
   * @param user The user who initiated the drop catalog operation.
   * @param identifier The identifier of the catalog that was attempted to be dropped.
   * @param isExists A boolean flag indicating whether the catalog existed at the time of the drop
   *     operation.
   */
  public DropCatalogEvent(String user, NameIdentifier identifier, boolean isExists) {
    super(user, identifier);
    this.isExists = isExists;
  }

  /**
   * Retrieves the existence status of the catalog at the time of the drop operation.
   *
   * @return A boolean value indicating whether the catalog existed. {@code true} if the catalog
   *     existed, otherwise {@code false}.
   */
  public boolean isExists() {
    return isExists;
  }
}
