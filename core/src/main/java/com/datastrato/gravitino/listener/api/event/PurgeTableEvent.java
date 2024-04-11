/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that occurs after a table is successfully purged from the database. This
 * class extends {@link TableEvent} to encapsulate specific details related to the purge operation
 * of a table. It captures both the existence status of the table at the time of the operation and
 * identifying information about the table and the user who initiated the purge.
 */
@DeveloperApi
public final class PurgeTableEvent extends TableEvent {
  private final boolean isExists;

  /**
   * Constructs a new {@code PurgeTableEvent} instance, providing details about the outcome of a
   * table purge operation.
   *
   * @param user The user who initiated the purge table operation. This information is crucial for
   *     auditing purposes and determining who is responsible for the deletion.
   * @param identifier The identifier of the table that was targeted for purging. This offers a
   *     direct reference to the specific table affected by the operation.
   * @param isExists A boolean indicator reflecting whether the table was present in the database at
   *     the time of the purge operation. This detail helps to understand the pre-operation state of
   *     the database with respect to the table's existence.
   */
  public PurgeTableEvent(String user, NameIdentifier identifier, boolean isExists) {
    super(user, identifier);
    this.isExists = isExists;
  }

  /**
   * Retrieves the status of the table's existence at the time of the purge operation.
   *
   * @return A boolean value indicating the table's existence status. {@code true} signifies that
   *     the table was present before the operation, {@code false} indicates it was not.
   */
  public boolean isExists() {
    return isExists;
  }
}
