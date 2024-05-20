/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.TableInfo;

/**
 * Represents an event that is generated when an attempt to create a table fails due to an
 * exception.
 */
@DeveloperApi
public final class CreateTableFailureEvent extends TableFailureEvent {
  private final TableInfo createTableRequest;

  /**
   * Constructs a {@code CreateTableFailureEvent} instance, capturing detailed information about the
   * failed table creation attempt.
   *
   * @param user The user who initiated the table creation operation.
   * @param identifier The identifier of the table that was attempted to be created.
   * @param exception The exception that was thrown during the table creation operation, providing
   *     insight into what went wrong.
   * @param createTableRequest The original request information used to attempt to create the table.
   *     This includes details such as the intended table schema, properties, and other
   *     configuration options that were specified.
   */
  public CreateTableFailureEvent(
      String user, NameIdentifier identifier, Exception exception, TableInfo createTableRequest) {
    super(user, identifier, exception);
    this.createTableRequest = createTableRequest;
  }

  /**
   * Retrieves the original request information for the attempted table creation.
   *
   * @return The {@link TableInfo} instance representing the request information for the failed
   *     table creation attempt.
   */
  public TableInfo createTableRequest() {
    return createTableRequest;
  }
}
