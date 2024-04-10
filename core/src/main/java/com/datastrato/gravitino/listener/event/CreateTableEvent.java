/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.info.TableInfo;

/**
 * Represents an event triggered upon the successful creation of a table. This class extends {@link
 * TableEvent} to provide detailed information specifically related to the table's creation. This
 * includes the final table information as it is returned to the user once the creation process has
 * successfully completed.
 *
 * <p>Such an event is instrumental for a variety of purposes including, but not limited to,
 * auditing activities associated with table creation, monitoring the creation of tables within a
 * system, and acquiring insights into the final configuration and state of a newly created table.
 */
@DeveloperApi
public class CreateTableEvent extends TableEvent {
  private final TableInfo createdTableInfo;

  /**
   * Constructs an instance of {@code CreateTableEvent}, capturing essential details about the
   * successful creation of a table.
   *
   * <p>This constructor documents the successful culmination of the table creation process by
   * encapsulating the final state of the table. This includes any adjustments or resolutions made
   * to the table's properties or configuration during the creation process.
   *
   * @param user The username of the individual who initiated the table creation. This information
   *     is vital for tracking the source of changes and for audit trails.
   * @param identifier The unique identifier of the table that was created, providing a precise
   *     reference to the affected table.
   * @param createdTableInfo The final state of the table post-creation. This information is
   *     reflective of the table's configuration, including any default settings or properties
   *     applied during the creation process.
   */
  public CreateTableEvent(String user, NameIdentifier identifier, TableInfo createdTableInfo) {
    super(user, identifier);
    this.createdTableInfo = createdTableInfo;
  }

  /**
   * Retrieves the final state and configuration information of the table as it was returned to the
   * user after successful creation.
   *
   * @return A {@link TableInfo} instance encapsulating the comprehensive details of the newly
   *     created table, highlighting its configuration and any default settings applied.
   */
  public TableInfo getCreatedTableInfo() {
    return createdTableInfo;
  }
}
