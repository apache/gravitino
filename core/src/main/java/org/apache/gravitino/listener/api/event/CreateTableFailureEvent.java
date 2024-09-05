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
import org.apache.gravitino.listener.api.info.TableInfo;

/**
 * Represents an event that is generated when an attempt to create a table fails due to an
 * exception.
 */
@DeveloperApi
public final class CreateTableFailureEvent extends TableFailureEvent {
  private final TableInfo createTableRequest;

  /**
   * Constructs a {@code CreateTableFailureEvent} instance, capturing detailed information about the
   * failed table creation attempt.
   *
   * @param user The user who initiated the table creation operation.
   * @param identifier The identifier of the table that was attempted to be created.
   * @param exception The exception that was thrown during the table creation operation, providing
   *     insight into what went wrong.
   * @param createTableRequest The original request information used to attempt to create the table.
   *     This includes details such as the intended table schema, properties, and other
   *     configuration options that were specified.
   */
  public CreateTableFailureEvent(
      String user, NameIdentifier identifier, Exception exception, TableInfo createTableRequest) {
    super(user, identifier, exception);
    this.createTableRequest = createTableRequest;
  }

  /**
   * Retrieves the original request information for the attempted table creation.
   *
   * @return The {@link TableInfo} instance representing the request information for the failed
   *     table creation attempt.
   */
  public TableInfo createTableRequest() {
    return createTableRequest;
  }
}
