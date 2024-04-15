/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.rel.TableChange;

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
