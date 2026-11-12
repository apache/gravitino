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

import javax.annotation.Nullable;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.info.CatalogInfo;

/**
 * Represents an event that is triggered when a catalog connection test fails.
 *
 * <p>Request parameters and the original exception are available without redaction, consistent with
 * other catalog events. Listeners are responsible for redacting sensitive values before forwarding
 * or logging them.
 */
@DeveloperApi
public final class TestConnectionFailureEvent extends CatalogFailureEvent {
  @Nullable private final CatalogInfo catalogInfo;
  @Nullable private final CatalogChange[] catalogChanges;

  /**
   * Constructs an instance of {@code TestConnectionFailureEvent}.
   *
   * @param user The username of the individual who initiated the connection test.
   * @param identifier The identifier of the catalog whose connection test failed.
   * @param exception The original exception encountered during the connection test.
   */
  public TestConnectionFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    this(user, identifier, exception, null, null);
  }

  /**
   * Constructs a failure event with the parameters supplied for the connection test.
   *
   * @param user The username of the individual who initiated the connection test.
   * @param identifier The identifier of the catalog whose connection test failed.
   * @param exception The original exception encountered during the connection test.
   * @param catalogInfo The parameters of a pre-create test, or null for an existing catalog.
   * @param catalogChanges The proposed changes, or null when no changes were supplied.
   */
  public TestConnectionFailureEvent(
      String user,
      NameIdentifier identifier,
      Exception exception,
      @Nullable CatalogInfo catalogInfo,
      @Nullable CatalogChange[] catalogChanges) {
    super(user, identifier, exception);
    this.catalogInfo = catalogInfo;
    this.catalogChanges = catalogChanges == null ? null : catalogChanges.clone();
  }

  /**
   * Returns the catalog parameters supplied for a pre-create connection test.
   *
   * @return The catalog parameters, or null for an existing-catalog test.
   */
  @Nullable
  public CatalogInfo catalogInfo() {
    return catalogInfo;
  }

  /**
   * Returns the proposed changes supplied for an existing-catalog connection test.
   *
   * @return The proposed changes, or null when no changes were supplied.
   */
  @Nullable
  public CatalogChange[] catalogChanges() {
    return catalogChanges;
  }

  @Override
  public OperationType operationType() {
    return OperationType.TEST_CONNECTION_CATALOG;
  }
}
