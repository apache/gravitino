/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.rel.Table;

/**
 * Represents an event triggered after a table is successfully created. This class extends {@link
 * TableEvent} to encapsulate specific details related to the creation of a table, including both
 * the request used to initiate the table creation and the resulting table information provided to
 * the user upon successful creation.
 *
 * <p>This event can be useful for auditing operations, tracking table creation activities, or
 * understanding the configuration and state of a newly created table.
 */
@DeveloperApi
public class CreateTableEvent extends TableEvent {
  private Table createdTableInfo;
  private Table createTableRequest;

  /**
   * Constructs a {@code CreateTableEvent} instance, capturing details about a successful table
   * creation operation.
   *
   * <p>This constructor initializes the event with comprehensive information about the table
   * creation, including who initiated it, the unique identifier of the created table, the original
   * request parameters, and the final state of the created table.
   *
   * @param user The username of the individual who initiated the table creation operation. This
   *     parameter is essential for auditing purposes and tracking responsibility for changes.
   * @param identifier The unique identifier of the table that was created, providing a clear
   *     reference to the affected table.
   * @param createTableRequest The original request data used to create the table. This includes
   *     details such as the table's schema, properties, and other configuration options specified
   *     by the user during the creation process.
   * @param table The table information returned upon successful creation. This represents the final
   *     state of the table, including any defaults or properties resolved during the creation
   *     process.
   */
  public CreateTableEvent(
      String user, NameIdentifier identifier, Table createTableRequest, Table table) {
    super(user, identifier);
    this.createTableRequest = createTableRequest;
    this.createdTableInfo = table;
  }

  /**
   * Retrieves the final state and information of the table as returned to the user after successful
   * creation.
   *
   * @return The {@link Table} instance representing the final state of the newly created table.
   */
  public Table getCreatedTableInfo() {
    return createdTableInfo;
  }

  /**
   * Retrieves the original request data used to create the table.
   *
   * <p>This information is beneficial for auditing, understanding the user's initial configuration
   * intentions, and for potential troubleshooting of table creation operations.
   *
   * @return The {@link Table} instance representing the initial request data for the table
   *     creation.
   */
  public Table getCreateTableRequest() {
    return createTableRequest;
  }
}
