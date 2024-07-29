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

/**
 * Represents an event that is generated when an attempt to create a catalog fails due to an
 * exception.
 */
@DeveloperApi
public final class CreateCatalogFailureEvent extends CatalogFailureEvent {
  private final CatalogInfo createCatalogRequest;

  /**
   * Constructs a {@code CreateCatalogFailureEvent} instance, capturing detailed information about
   * the failed catalog creation attempt.
   *
   * @param user The user who initiated the catalog creation operation.
   * @param identifier The identifier of the catalog that was attempted to be created.
   * @param exception The exception that was thrown during the catalog creation operation, providing
   *     insight into what went wrong.
   * @param createCatalogRequest The original request information used to attempt to create the
   *     catalog. This includes details such as the intended catalog schema, properties, and other
   *     configuration options that were specified.
   */
  public CreateCatalogFailureEvent(
      String user,
      NameIdentifier identifier,
      Exception exception,
      CatalogInfo createCatalogRequest) {
    super(user, identifier, exception);
    this.createCatalogRequest = createCatalogRequest;
  }

  /**
   * Retrieves the original request information for the attempted catalog creation.
   *
   * @return The {@link CatalogInfo} instance representing the request information for the failed
   *     catalog creation attempt.
   */
  public CatalogInfo createCatalogRequest() {
    return createCatalogRequest;
  }
}
