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
 * This class extends {@link TableFailureEvent} to specifically address failure scenarios
 * encountered during the table alteration process. It encapsulates the exception that led to the
 * failure along with the changes that were attempted, providing a comprehensive context for
 * understanding the failure.
 */
@DeveloperApi
public final class AlterTableFailureEvent extends TableFailureEvent {
  private final TableChange[] tableChanges;

  /**
   * Constructs an {@code AlterTableFailureEvent} instance, capturing detailed information about the
   * failed table alteration attempt.
   *
   * @param user The user who initiated the table alteration operation. This information is crucial
   *     for auditing and pinpointing the cause of the failure.
   * @param identifier The identifier of the table that was attempted to be altered. This aids in
   *     identifying the specific table associated with the failure.
   * @param exception The exception that was thrown during the table alteration operation, shedding
   *     light on the nature of the failure.
   * @param tableChanges The changes that were attempted on the table. This array of {@link
   *     TableChange} objects provides insight into what modifications were intended, offering a
   *     clear picture of the alteration attempt.
   */
  public AlterTableFailureEvent(
      String user, NameIdentifier identifier, Exception exception, TableChange[] tableChanges) {
    super(user, identifier, exception);
    this.tableChanges = tableChanges.clone();
  }

  /**
   * Retrieves the changes that were attempted on the table, leading to the failure. This provides
   * valuable information for diagnosing the failure and understanding the intended alterations that
   * were not successfully applied.
   *
   * @return An array of {@link TableChange} objects representing the attempted modifications to the
   *     table.
   */
  public TableChange[] tableChanges() {
    return tableChanges;
  }
}
