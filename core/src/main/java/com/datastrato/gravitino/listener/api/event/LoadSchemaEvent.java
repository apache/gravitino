/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.SchemaInfo;

/** Represents an event triggered upon the successful loading of a schema. */
@DeveloperApi
public final class LoadSchemaEvent extends SchemaEvent {
  private final SchemaInfo loadedSchemaInfo;

  public LoadSchemaEvent(String user, NameIdentifier identifier, SchemaInfo loadedSchemaInfo) {
    super(user, identifier);
    this.loadedSchemaInfo = loadedSchemaInfo;
  }

  /**
   * Retrieves the state of the schema as it was made available to the user after successful
   * loading.
   *
   * @return A {@link SchemaInfo} instance encapsulating the details of the schema as loaded.
   */
  public SchemaInfo loadedSchemaInfo() {
    return loadedSchemaInfo;
  }
}
