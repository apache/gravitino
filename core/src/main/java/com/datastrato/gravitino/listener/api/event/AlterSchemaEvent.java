/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.SchemaChange;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.SchemaInfo;

/** Represents an event fired when a schema is successfully altered. */
@DeveloperApi
public final class AlterSchemaEvent extends SchemaEvent {
  private final SchemaChange[] schemaChanges;
  private final SchemaInfo updatedSchemaInfo;

  public AlterSchemaEvent(
      String user,
      NameIdentifier identifier,
      SchemaChange[] schemaChanges,
      SchemaInfo updatedSchemaInfo) {
    super(user, identifier);
    this.schemaChanges = schemaChanges.clone();
    this.updatedSchemaInfo = updatedSchemaInfo;
  }

  /**
   * Retrieves the updated state of the schema after the successful alteration.
   *
   * @return A {@link SchemaInfo} instance encapsulating the details of the altered schema.
   */
  public SchemaInfo updatedSchemaInfo() {
    return updatedSchemaInfo;
  }

  /**
   * Retrieves the specific changes that were made to the schema during the alteration process.
   *
   * @return An array of {@link SchemaChange} objects detailing each modification applied to the
   *     schema.
   */
  public SchemaChange[] schemaChanges() {
    return schemaChanges;
  }
}
