/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.rel.SchemaChange;

/**
 * Represents an event that is triggered when an attempt to alter a schema fails due to an
 * exception.
 */
@DeveloperApi
public final class AlterSchemaFailureEvent extends SchemaFailureEvent {
  public AlterSchemaFailureEvent(
      String user, NameIdentifier identifier, Exception exception, SchemaChange[] schemaChanges) {
    super(user, identifier, exception);
  }
}
