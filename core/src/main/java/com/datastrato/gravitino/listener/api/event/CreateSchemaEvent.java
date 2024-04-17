/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.SchemaInfo;

/** Represents an event triggered upon the successful creation of a schema. */
@DeveloperApi
public final class CreateSchemaEvent extends SchemaEvent {
  private final SchemaInfo createdSchemaInfo;

  public CreateSchemaEvent(String user, NameIdentifier identifier, SchemaInfo schemaInfo) {
    super(user, identifier);
    this.createdSchemaInfo = schemaInfo;
  }

  /**
   * Retrieves the final state of the schema as it was returned to the user after successful
   * creation.
   *
   * @return A {@link SchemaInfo} instance encapsulating the comprehensive details of the newly
   *     created schema.
   */
  public SchemaInfo createdSchemaInfo() {
    return createdSchemaInfo;
  }
}
