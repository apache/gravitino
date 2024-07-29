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
import org.apache.gravitino.rel.TableChange;

/**
 * Represents an event that is triggered when an attempt to alter a table fails due to an exception.
 */
@DeveloperApi
public final class AlterTableFailureEvent extends TableFailureEvent {
  private final TableChange[] tableChanges;

  /**
   * Constructs an {@code AlterTableFailureEvent} instance, capturing detailed information about the
   * failed table alteration attempt.
   *
   * @param user The user who initiated the table alteration operation.
   * @param identifier The identifier of the table that was attempted to be altered.
   * @param exception The exception that was thrown during the table alteration operation.
   * @param tableChanges The changes that were attempted on the table.
   */
  public AlterTableFailureEvent(
      String user, NameIdentifier identifier, Exception exception, TableChange[] tableChanges) {
    super(user, identifier, exception);
    this.tableChanges = tableChanges.clone();
  }

  /**
   * Retrieves the changes that were attempted on the table.
   *
   * @return An array of {@link TableChange} objects representing the attempted modifications to the
   *     table.
   */
  public TableChange[] tableChanges() {
    return tableChanges;
  }
}
