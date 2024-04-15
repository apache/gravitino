/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.TableInfo;

/** Represents an event triggered upon the successful loading of a table. */
@DeveloperApi
public final class LoadTableEvent extends TableEvent {
  private final TableInfo loadedTableInfo;

  /**
   * Constructs an instance of {@code LoadTableEvent}.
   *
   * @param user The username of the individual who initiated the table loading.
   * @param identifier The unique identifier of the table that was loaded.
   * @param tableInfo The state of the table post-loading.
   */
  public LoadTableEvent(String user, NameIdentifier identifier, TableInfo tableInfo) {
    super(user, identifier);
    this.loadedTableInfo = tableInfo;
  }

  /**
   * Retrieves the state of the table as it was made available to the user after successful loading.
   *
   * @return A {@link TableInfo} instance encapsulating the details of the table as loaded.
   */
  public TableInfo loadedTableInfo() {
    return loadedTableInfo;
  }
}
