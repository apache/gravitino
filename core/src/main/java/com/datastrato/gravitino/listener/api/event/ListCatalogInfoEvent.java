/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.annotation.DeveloperApi;

/** Represents an event that is triggered upon the successful list of catalog Info. */
@DeveloperApi
public final class ListCatalogInfoEvent extends CatalogEvent {
  private final Namespace namespace;

  public ListCatalogInfoEvent(String user, Namespace namespace) {
    super(user, NameIdentifier.of(namespace.toString()));
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
