/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is generated when an attempt to drop a schema fails due to an exception.
 */
@DeveloperApi
public final class DropSchemaFailureEvent extends SchemaFailureEvent {
  private final boolean cascade;

  public DropSchemaFailureEvent(
      String user, NameIdentifier identifier, Exception exception, boolean cascade) {
    super(user, identifier, exception);
    this.cascade = cascade;
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
