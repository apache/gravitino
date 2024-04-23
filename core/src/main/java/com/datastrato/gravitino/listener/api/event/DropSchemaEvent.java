/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/** Represents an event that is generated after a schema is successfully dropped. */
@DeveloperApi
public final class DropSchemaEvent extends SchemaEvent {
  private final boolean isExists;
  private final boolean cascade;

  public DropSchemaEvent(
      String user, NameIdentifier identifier, boolean isExists, boolean cascade) {
    super(user, identifier);
    this.isExists = isExists;
    this.cascade = cascade;
  }

  /**
   * Retrieves the existence status of the schema at the time of the drop operation.
   *
   * @return A boolean value indicating whether the schema existed. {@code true} if the schema
   *     existed, otherwise {@code false}.
   */
  public boolean isExists() {
    return isExists;
  }

  /**
   * Indicates whether the drop operation was performed with a cascade option.
   *
   * @return A boolean value indicating whether the drop operation was set to cascade. If {@code
   *     true}, dependent objects such as tables and views within the schema were also dropped.
   *     Otherwise, the operation would fail if the schema contained any dependent objects.
   */
  public boolean cascade() {
    return cascade;
  }
}
