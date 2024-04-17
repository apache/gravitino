/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is generated after a table is successfully dropped from the database.
 */
@DeveloperApi
public final class DropTableEvent extends TableEvent {
  private final boolean isExists;

  /**
   * Constructs a new {@code DropTableEvent} instance, encapsulating information about the outcome
   * of a table drop operation.
   *
   * @param user The user who initiated the drop table operation.
   * @param identifier The identifier of the table that was attempted to be dropped.
   * @param isExists A boolean flag indicating whether the table existed at the time of the drop
   *     operation.
   */
  public DropTableEvent(String user, NameIdentifier identifier, boolean isExists) {
    super(user, identifier);
    this.isExists = isExists;
  }

  /**
   * Retrieves the existence status of the table at the time of the drop operation.
   *
   * @return A boolean value indicating whether the table existed. {@code true} if the table
   *     existed, otherwise {@code false}.
   */
  public boolean isExists() {
    return isExists;
  }
}
