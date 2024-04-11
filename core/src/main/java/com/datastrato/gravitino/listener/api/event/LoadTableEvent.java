/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.TableInfo;

/**
 * Represents an event triggered upon the successful loading of a table. This class extends {@link
 * TableEvent} to provide detailed information specifically related to the loading of the table.
 * This includes the table information as it is available to the user after the loading process has
 * been completed.
 *
 * <p>This event is crucial for a range of activities, including, but not limited to, auditing the
 * process associated with table loading, monitoring the loading of tables within a system, and
 * gaining insights into the configuration and state of a table that has been newly loaded.
 */
@DeveloperApi
public final class LoadTableEvent extends TableEvent {
  private final TableInfo loadedTableInfo;

  /**
   * Constructs an instance of {@code LoadTableEvent}, capturing essential details about the
   * successful loading of a table.
   *
   * <p>This constructor records the successful completion of the table loading process by
   * encapsulating the state of the table at the time of loading. This includes any adjustments or
   * configurations applied to the table's properties during the loading process.
   *
   * @param user The username of the individual who initiated the table loading. This information is
   *     crucial for tracing the origin of operations and for audit trails.
   * @param identifier The unique identifier of the table that was loaded, providing a clear
   *     reference to the affected table.
   * @param tableInfo The state of the table post-loading. This information reflects the table's
   *     configuration, including any default settings or properties applied during the loading
   *     process.
   */
  public LoadTableEvent(String user, NameIdentifier identifier, TableInfo tableInfo) {
    super(user, identifier);
    this.loadedTableInfo = tableInfo;
  }

  /**
   * Retrieves the state and configuration information of the table as it was made available to the
   * user after successful loading.
   *
   * @return A {@link TableInfo} instance encapsulating the details of the table as loaded,
   *     highlighting its configuration and any default settings applied.
   */
  public TableInfo loadedTableInfo() {
    return loadedTableInfo;
  }
}
