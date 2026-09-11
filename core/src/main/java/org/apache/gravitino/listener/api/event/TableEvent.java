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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Represents an abstract base class for events related to table operations. This class extends
 * {@link Event} to provide a more specific context involving operations on tables, such as
 * creation, deletion, or modification. It captures essential information including the user
 * performing the operation and the identifier of the table being operated on.
 *
 * <p>Concrete implementations of this class should provide additional details pertinent to the
 * specific type of table operation being represented.
 */
@DeveloperApi
public abstract class TableEvent extends Event {
  private final Map<String, String> customInfo;

  /**
   * Constructs a new {@code TableEvent} with the specified user and table identifier.
   *
   * @param user The user responsible for triggering the table operation.
   * @param identifier The identifier of the table involved in the operation. This encapsulates
   *     details such as the metalake, catalog, schema, and table name.
   */
  protected TableEvent(String user, NameIdentifier identifier) {
    this(user, identifier, ImmutableMap.of());
  }

  /**
   * Constructs a new {@code TableEvent} with optional audit extras.
   *
   * @param user The user responsible for triggering the table operation.
   * @param identifier The identifier of the table involved in the operation.
   * @param customInfo optional audit facts contributed by an inner dispatcher
   */
  protected TableEvent(String user, NameIdentifier identifier, Map<String, String> customInfo) {
    super(user, identifier);
    this.customInfo =
        customInfo == null || customInfo.isEmpty()
            ? ImmutableMap.of()
            : ImmutableMap.copyOf(customInfo);
  }

  @Override
  public OperationStatus operationStatus() {
    return OperationStatus.SUCCESS;
  }

  /** {@inheritDoc} */
  @Override
  protected Map<String, String> ownCustomInfo() {
    return customInfo;
  }
}
