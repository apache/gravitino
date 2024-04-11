/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.TableInfo;
import com.datastrato.gravitino.rel.TableChange;

/**
 * Represents an event fired when a table is successfully altered. This class inherits from {@link
 * TableEvent} and provides additional information pertinent to the modifications made to a table.
 * This includes the resulting table information as provided to the user after the alteration is
 * executed.
 *
 * <p>This event is crucial for several activities, including but not limited to, tracking and
 * auditing changes made to table structures, monitoring modifications within a system's tables, and
 * gaining insights into the updated configuration and state of an altered table.
 */
@DeveloperApi
public final class AlterTableEvent extends TableEvent {
  private final TableInfo updatedTableInfo;
  private final TableChange[] tableChanges;

  /**
   * Constructs an instance of {@code AlterTableEvent}, encapsulating the key details about the
   * successful alteration of a table.
   *
   * <p>This constructor captures the state of the table following its alteration, including any
   * property adjustments or configuration changes that occurred during the process.
   *
   * @param user The username of the individual responsible for initiating the table alteration.
   *     This detail is essential for tracing the origins of changes and establishing audit records.
   * @param identifier The unique identifier of the altered table, serving as a clear reference
   *     point for the table in question.
   * @param tableChanges An array of {@link TableChange} objects representing the specific changes
   *     applied to the table during the alteration process.
   * @param updatedTableInfo The post-alteration state of the table. This reflects the table's
   *     configuration after the applied changes, including any default settings or properties that
   *     were adjusted.
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
   * Retrieves the updated state and configuration information of the table after the successful
   * alteration.
   *
   * @return A {@link TableInfo} instance encapsulating the details of the altered table,
   *     highlighting the modifications made and the current table configuration.
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
