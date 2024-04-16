/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/** Represents an abstract base class for events related to schema operations. */
@DeveloperApi
public abstract class SchemaEvent extends Event {
  protected SchemaEvent(String user, NameIdentifier identifier) {
    super(user, identifier);
  }
}
