/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is triggered when an attempt to list catalogs within a namespace fails
 * due to an exception.
 */
@DeveloperApi
public final class ListCatalogFailureEvent extends CatalogFailureEvent {
  private final Namespace namespace;

  public ListCatalogFailureEvent(String user, Exception exception, Namespace namespace) {
    super(user, NameIdentifier.of(namespace.toString()), exception);
    this.namespace = namespace;
  }

  /**
   * Provides the namespace associated with this event.
   *
   * @return A {@link Namespace} instance from which catalogs were listed.
   */
  public Namespace namespace() {
    return namespace;
  }
}
