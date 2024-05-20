/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.SchemaInfo;

/**
 * Represents an event that is generated when an attempt to create a schema fails due to an
 * exception.
 */
@DeveloperApi
public final class CreateSchemaFailureEvent extends SchemaFailureEvent {
  private final SchemaInfo createSchemaRequest;

  public CreateSchemaFailureEvent(
      String user, NameIdentifier identifier, Exception exception, SchemaInfo createSchemaRequest) {
    super(user, identifier, exception);
    this.createSchemaRequest = createSchemaRequest;
  }

  /**
   * Retrieves the original request information for the attempted schema creation.
   *
   * @return The {@link SchemaInfo} instance representing the request information for the failed
   *     schema creation attempt.
   */
  public SchemaInfo createSchemaRequest() {
    return createSchemaRequest;
  }
}
