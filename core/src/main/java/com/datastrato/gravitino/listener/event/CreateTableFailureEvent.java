/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.rel.Table;

/**
 * Represents an event that is generated when an attempt to create a table fails due to an
 * exception. This class extends {@link TableFailureEvent} to specifically address failure scenarios
 * encountered during the table creation process. It encapsulates both the exception that caused the
 * failure and the original request details for the table creation, providing comprehensive context
 * for the failure.
 */
@DeveloperApi
public class CreateTableFailureEvent extends TableFailureEvent {
  private Table createTableRequest;

  /**
   * Constructs a {@code CreateTableFailureEvent} instance, capturing detailed information about the
   * failed table creation attempt.
   *
   * @param user The user who initiated the table creation operation. This information is essential
   *     for auditing and diagnosing the cause of the failure.
   * @param identifier The identifier of the table that was attempted to be created. This helps in
   *     pinpointing the specific table related to the failure.
   * @param exception The exception that was thrown during the table creation operation, providing
   *     insight into what went wrong.
   * @param createTableRequest The original request information used to attempt to create the table.
   *     This includes details such as the intended table schema, properties, and other
   *     configuration options that were specified.
   */
  public CreateTableFailureEvent(
      String user, NameIdentifier identifier, Exception exception, Table createTableRequest) {
    super(user, identifier, exception);
    this.createTableRequest = createTableRequest;
  }

  /**
   * Retrieves the original request information for the attempted table creation. This information
   * can be valuable for understanding the configuration and expectations that led to the failure,
   * facilitating analysis and potential corrective actions.
   *
   * @return The {@link Table} instance representing the request information for the failed table
   *     creation attempt.
   */
  public Table getCreateTableRequest() {
    return createTableRequest;
  }
}
