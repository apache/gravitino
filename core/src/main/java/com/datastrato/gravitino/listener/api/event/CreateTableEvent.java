/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.TableInfo;

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
