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
import org.apache.gravitino.rel.TableChange;

/** Represents an event fired when a table is successfully altered. */
@DeveloperApi
public final class AlterTableEvent extends TableEvent {
  private final TableInfo updatedTableInfo;
  private final TableChange[] tableChanges;

  /**
   * Constructs an instance of {@code AlterTableEvent}, encapsulating the key details about the
   * successful alteration of a table.
   *
   * @param user The username of the individual responsible for initiating the table alteration.
   * @param identifier The unique identifier of the altered table, serving as a clear reference
   *     point for the table in question.
   * @param tableChanges An array of {@link TableChange} objects representing the specific changes
   *     applied to the table during the alteration process.
   * @param updatedTableInfo The post-alteration state of the table.
   */
  public AlterTableEvent(
      String user,
      NameIdentifier identifier,
      TableChange[] tableChanges,
      TableInfo updatedTableInfo) {
    super(user, identifier);
    this.tableChanges = tableChanges.clone();
    this.updatedTableInfo = updatedTableInfo;
  }

  /**
   * Retrieves the updated state of the table after the successful alteration.
   *
   * @return A {@link TableInfo} instance encapsulating the details of the altered table.
   */
  public TableInfo updatedTableInfo() {
    return updatedTableInfo;
  }

  /**
   * Retrieves the specific changes that were made to the table during the alteration process.
   *
   * @return An array of {@link TableChange} objects detailing each modification applied to the
   *     table.
   */
  public TableChange[] tableChanges() {
    return tableChanges;
  }
}
