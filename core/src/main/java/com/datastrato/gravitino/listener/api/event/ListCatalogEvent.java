/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.annotation.DeveloperApi;

/** Represents an event that is triggered upon the successful list of catalogs. */
@DeveloperApi
public final class ListCatalogEvent extends CatalogEvent {
  private final Namespace namespace;

  /**
   * Constructs an instance of {@code ListCatalogEvent}.
   *
   * @param user The username of the individual who initiated the catalog listing.
   * @param namespace The namespace from which catalogs were listed.
   */
  public ListCatalogEvent(String user, Namespace namespace) {
    super(user, NameIdentifier.of(namespace.levels()));
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
