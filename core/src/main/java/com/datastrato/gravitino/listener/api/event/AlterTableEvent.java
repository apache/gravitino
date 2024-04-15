/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.TableInfo;
import com.datastrato.gravitino.rel.TableChange;

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
