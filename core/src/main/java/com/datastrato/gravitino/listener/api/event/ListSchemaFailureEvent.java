/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is triggered when an attempt to list schemas within a namespace fails
 * due to an exception.
 */
@DeveloperApi
public final class ListSchemaFailureEvent extends SchemaFailureEvent {
  private final Namespace namespace;

  public ListSchemaFailureEvent(String user, Namespace namespace, Exception exception) {
    super(user, NameIdentifier.of(namespace.levels()), exception);
    this.namespace = namespace;
  }

  /**
   * Retrieves the namespace associated with this failure event.
   *
   * @return A {@link Namespace} instance for which the schema listing was attempted
   */
  public Namespace namespace() {
    return namespace;
  }
}
