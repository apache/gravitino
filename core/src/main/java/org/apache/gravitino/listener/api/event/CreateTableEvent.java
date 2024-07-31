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

/** Represents an event triggered upon the successful creation of a table. */
@DeveloperApi
public final class CreateTableEvent extends TableEvent {
  private final TableInfo createdTableInfo;

  /**
   * Constructs an instance of {@code CreateTableEvent}, capturing essential details about the
   * successful creation of a table.
   *
   * @param user The username of the individual who initiated the table creation.
   * @param identifier The unique identifier of the table that was created.
   * @param createdTableInfo The final state of the table post-creation.
   */
  public CreateTableEvent(String user, NameIdentifier identifier, TableInfo createdTableInfo) {
    super(user, identifier);
    this.createdTableInfo = createdTableInfo;
  }

  /**
   * Retrieves the final state of the table as it was returned to the user after successful
   * creation.
   *
   * @return A {@link TableInfo} instance encapsulating the comprehensive details of the newly
   *     created table.
   */
  public TableInfo createdTableInfo() {
    return createdTableInfo;
  }
}
